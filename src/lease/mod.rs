use std::{
    sync::{
        atomic::{AtomicBool, AtomicU8, Ordering},
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
    lease_client, AuthClient, AuthedChannel, LeaseClient, LeaseGrantRequest, LeaseKeepAliveRequest,
    LeaseKeepAliveResponse, LeaseRevokeRequest,
};
use tokio_stream::{wrappers::UnboundedReceiverStream, StreamExt};
use tonic::{Status, Streaming};

struct LeaseHandleInner {
    // need to figure out how to signal that the lease is dead?
    lease_notify: Notify,
    lease_released: AtomicBool,
    lease_id: i64,
}

#[derive(Clone)]
struct LeaseHandle {
    inner: Arc<LeaseHandleInner>,
    dropped_sender: Arc<oneshot::Sender<()>>,
}

impl LeaseHandle {
    fn lease_id(&self) -> i64 {
        self.inner.lease_id
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

struct LeaseWorker {
    lease_client: LeaseClient<AuthedChannel>,
    handle_inner: Arc<LeaseHandleInner>,
    dropped_receiver: oneshot::Receiver<_>,
    state: LeaseWorkerState,
    interval: Interval,
    timeout_at: Instant,
    ttl: Duration,
}

impl LeaseWorker {
    async fn grant(
        lease_client: LeaseClient<AuthedChannel>,
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
        let inner = Arc::new(LeaseHandleInner {
            lease_id: lease.id,
            lease_notify: Notify::new(),
            lease_released: AtomicBool::new(false),
        });
        let handle = LeaseHandle {
            inner,
            dropped_sender: Arc::new(dropped_sender),
        };

        let mut interval = interval(Duration::from_secs((ttl.as_secs_f64() * 0.75) as _));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let worker = LeaseWorker {
            lease_client,
            handle_inner: inner.clone(),
            dropped_receiver,
            interval,
            state: LeaseWorkerState::NoStream,
            timeout_at: Instant::now() + ttl,
            ttl,
        };

        tokio::task::spawn(worker.run());

        Ok(handle)
    }

    async fn run(mut self) {
        loop {
            match self.state {
                LeaseWorkerState::NoStream => {
                    if self.dropped_receiver.try_recv().is_err() {
                        self.revoke_lease().await;
                        break;
                    }

                    let (tx, rx) = unbounded_channel();
                    tx.send(LeaseKeepAliveRequest {
                        id: self.handle_inner.lease_id,
                    })
                    .ok();

                    let mut lease_ka_streaming = match self
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
                                "error establishing lease channel for lease: {}",
                                self.handle_inner.lease_id
                            );
                            sleep(Duration::from_secs(3)).await;
                        }
                    };
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
                                id: self.handle_inner.lease_id,
                            })
                            .ok();
                        }
                        Action::KeepAliveResponse(None) => {
                            tracing::info!(
                                "lease {} keep-alive channel closed, re-establishing",
                                self.handle_inner.lease_id
                            );
                            self.state = LeaseWorkerState::NoStream;
                        }
                        Action::KeepAliveResponse(Some(Err(status))) => {
                            tracing::error!(
                                "error renewing lease {}, keep-alive channel returned error: {:?}, will retry immediately",
                                self.handle_inner.lease_id,
                                status
                            );
                            self.interval.reset_immediately();
                        }
                        Action::KeepAliveResponse(Some(Ok(response))) => {
                            assert_eq!(
                                response.id, self.handle_inner.lease_id,
                                "bug: lease id does not match"
                            );

                            if response.ttl > 0 {
                                self.interval.reset_after(Duration::from_millis(
                                    (((response.ttl * 1000) as f32) * 0.5) as _,
                                ));
                                self.timeout_at =
                                    Instant::now() + Duration::from_secs(response.ttl as _);
                            } else {
                                self.state = LeaseWorkerState::Revoked;
                                break;
                            }
                        }
                    }
                }
                LeaseWorkerState::Revoked => {
                    self.handle_inner
                        .lease_released
                        .store(true, Ordering::Acquire);
                    self.handle_inner.lease_notify.notify_waiters();
                    break;
                }
            }
        }
    }

    async fn revoke_lease(self) {
        let revoke_future = async {
            loop {
                match self
                    .lease_client
                    .lease_revoke(LeaseRevokeRequest {
                        id: self.handle_inner.lease_id,
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
                tracing::warn!("revoked lease {} successfully.", self.handle_inner.lease_id);
            }
            Err(_) => {
                tracing::warn!(
                    "timed out trying to issue revoke lease {} call before the lease expired.",
                    self.handle_inner.lease_id
                );
            }
        }
    }
}
