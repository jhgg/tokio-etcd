use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use tokio::{
    sync::{
        mpsc::{unbounded_channel, UnboundedSender},
        oneshot, Notify,
    },
    time::{interval, sleep, timeout_at, Instant, Interval},
};
use tokio_etcd_grpc_client::{
    AuthedChannel, LeaseClient, LeaseGrantRequest, LeaseKeepAliveRequest, LeaseKeepAliveResponse,
    LeaseRevokeRequest,
};
use tokio_stream::{wrappers::UnboundedReceiverStream, StreamExt};
use tonic::{Status, Streaming};

struct LeaseHandleInner {
    /// Notify is updated when lease_released is changed.
    notify: Notify,
    revoked: AtomicBool,
    id: i64,
}

impl LeaseHandleInner {
    fn new(id: i64) -> Self {
        Self {
            id,
            notify: Notify::new(),
            revoked: AtomicBool::new(false),
        }
    }

    fn set_revoked(&self, released: bool) {
        self.revoked.store(released, Ordering::Release);
        self.notify.notify_waiters();
    }
}

#[derive(Clone)]
pub struct LeaseHandle {
    inner: Arc<LeaseHandleInner>,
    _dropped_sender: Arc<oneshot::Sender<()>>,
}

impl LeaseHandle {
    pub(crate) async fn grant(
        mut lease_client: LeaseClient<AuthedChannel>,
        ttl: Duration,
    ) -> Result<LeaseHandle, Status> {
        if ttl < Duration::from_secs(10) {
            return Err(Status::invalid_argument("ttl must be above 10 seconds"));
        }

        let lease = lease_client
            .lease_grant(LeaseGrantRequest {
                ttl: ttl.as_secs() as _,
                id: 0, // server will provide us a lease id.
            })
            .await?
            .into_inner();

        let (dropped_sender, dropped_receiver) = oneshot::channel();
        let handle_inner = Arc::new(LeaseHandleInner::new(lease.id));
        let handle = LeaseHandle {
            inner: handle_inner.clone(),
            _dropped_sender: Arc::new(dropped_sender),
        };

        let mut interval = interval(Duration::from_secs((ttl.as_secs_f64() * 0.75) as _));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let worker = LeaseWorker {
            lease_client,
            handle_inner,
            dropped_receiver,
            interval,
            state: LeaseWorkerState::NoStream,
            timeout_at: Instant::now() + ttl,
        };

        tokio::task::spawn(worker.run());

        Ok(handle)
    }

    fn released(&self) -> bool {
        self.inner.revoked.load(Ordering::Acquire)
    }

    /// Returns the lease id from the handle, or None if the lease has expired.
    pub fn lease_id(&self) -> Option<i64> {
        self.released().then_some(self.inner.id)
    }

    /// Monitors the lease, resolving if the lease for whatever reason was revoked or expired by the server
    /// (and thus should not be used anymore).
    pub async fn monitor(&self) {
        loop {
            if self.released() {
                break;
            }

            self.inner.notify.notified().await;
        }
    }
}

enum LeaseWorkerState {
    NoStream,
    HasStream {
        stream: Streaming<LeaseKeepAliveResponse>,
        out: UnboundedSender<LeaseKeepAliveRequest>,
    },
    Revoked,
}

impl LeaseWorker {
    async fn run(mut self) {
        loop {
            match &mut self.state {
                LeaseWorkerState::NoStream => {
                    // technically, we should race this with the ka request below.
                    if self.dropped_receiver.try_recv().is_err() {
                        self.revoke_lease().await;
                        break;
                    }

                    let (tx, rx) = unbounded_channel();
                    tx.send(LeaseKeepAliveRequest {
                        id: self.handle_inner.id,
                    })
                    .ok();

                    match self
                        .lease_client
                        .lease_keep_alive(UnboundedReceiverStream::new(rx))
                        .await
                    {
                        Ok(response) => {
                            self.state = LeaseWorkerState::HasStream {
                                stream: response.into_inner(),
                                out: tx,
                            };
                        }
                        Err(error) => {
                            tracing::error!(
                                "error establishing lease channel for lease: {}, error: {:?}",
                                self.handle_inner.id,
                                error
                            );
                            sleep(Duration::from_secs(3)).await;
                        }
                    }
                }
                LeaseWorkerState::HasStream { stream, out } => {
                    enum Action {
                        /// All copies of the handle have dropped, so we can go ahead and revoke the lease.
                        HandleDropped,
                        /// Lease renewal interval has elapsed, so we should send a lease keep alive.
                        RequestKeepAlive,
                        /// We got a keep-alive response from the server.
                        KeepAliveResponse(Option<Result<LeaseKeepAliveResponse, Status>>),
                    }

                    let action = tokio::select! {
                        _ = self.interval.tick() => Action::RequestKeepAlive,
                        _ = &mut self.dropped_receiver => Action::HandleDropped,
                        res = stream.next() => Action::KeepAliveResponse(res),
                    };

                    match action {
                        Action::HandleDropped => {
                            self.revoke_lease().await;
                            break;
                        }
                        Action::RequestKeepAlive => {
                            out.send(LeaseKeepAliveRequest {
                                id: self.handle_inner.id,
                            })
                            .ok();
                        }
                        Action::KeepAliveResponse(None) => {
                            tracing::info!(
                                "lease {} keep-alive channel closed, re-establishing",
                                self.handle_inner.id
                            );
                            self.state = LeaseWorkerState::NoStream;
                        }
                        Action::KeepAliveResponse(Some(Err(status))) => {
                            tracing::error!(
                                "error renewing lease {}, keep-alive channel returned error: {:?}, will retry immediately",
                                self.handle_inner.id,
                                status
                            );
                            self.interval.reset_immediately();
                        }
                        Action::KeepAliveResponse(Some(Ok(response))) => {
                            assert_eq!(
                                response.id, self.handle_inner.id,
                                "bug: lease id does not match"
                            );

                            if response.ttl > 0 {
                                tracing::info!(
                                    "lease {} has been kept-alive, new TTL: {}",
                                    self.handle_inner.id,
                                    response.ttl
                                );
                                self.interval.reset_after(Duration::from_millis(
                                    (((response.ttl * 1000) as f32) * 0.5) as _,
                                ));
                                self.timeout_at =
                                    Instant::now() + Duration::from_secs(response.ttl as _);
                            } else {
                                tracing::error!(
                                    "lease {} has expired before we could keep it alive",
                                    self.handle_inner.id
                                );
                                self.state = LeaseWorkerState::Revoked;
                                break;
                            }
                        }
                    }
                }
                LeaseWorkerState::Revoked => {
                    self.handle_inner.set_revoked(true);
                    break;
                }
            }
        }
    }

    async fn revoke_lease(mut self) {
        let revoke_future = async {
            loop {
                match self
                    .lease_client
                    .lease_revoke(LeaseRevokeRequest {
                        id: self.handle_inner.id,
                    })
                    .await
                {
                    Ok(res) => {
                        break res;
                    }
                    Err(_) => {
                        sleep(Duration::from_secs(1)).await;
                    }
                }
            }
        };

        match timeout_at(self.timeout_at, revoke_future).await {
            Ok(_) => {
                tracing::warn!("revoked lease {} successfully.", self.handle_inner.id);
            }
            Err(_) => {
                tracing::warn!(
                    "timed out trying to issue revoke lease {} call before the lease expired.",
                    self.handle_inner.id
                );
            }
        }
    }
}

struct LeaseWorker {
    lease_client: LeaseClient<AuthedChannel>,
    handle_inner: Arc<LeaseHandleInner>,
    dropped_receiver: oneshot::Receiver<()>,
    state: LeaseWorkerState,
    interval: Interval,
    timeout_at: Instant,
}
