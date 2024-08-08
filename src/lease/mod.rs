use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use tokio::{
    sync::{
        mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender, WeakUnboundedSender},
        oneshot, Notify,
    },
    task::JoinSet,
    time::{interval, sleep, timeout_at, Instant, Interval, Sleep},
};
use tokio_etcd_grpc_client::{
    AuthedChannel, LeaseClient, LeaseGrantRequest, LeaseKeepAliveRequest, LeaseKeepAliveResponse,
    LeaseRevokeRequest,
};
use tokio_stream::{wrappers::UnboundedReceiverStream, StreamExt};
use tokio_timer::{delay_queue::Key, DelayQueue};
use tonic::{Response, Status, Streaming};

struct LeaseRevokedNotify {
    notify: Notify,
    revoked: AtomicBool,
}
struct LeaseRevokedNotifyDropGuard {
    notify: Arc<LeaseRevokedNotify>,
}

impl Drop for LeaseRevokedNotifyDropGuard {
    fn drop(&mut self) {
        self.notify.set_revoked();
    }
}

impl LeaseRevokedNotifyDropGuard {}

impl LeaseRevokedNotify {
    fn new() -> (Arc<Self>, LeaseRevokedNotifyDropGuard) {
        let notify = Arc::new(Self {
            notify: Notify::new(),
            revoked: AtomicBool::new(false),
        });

        (notify.clone(), LeaseRevokedNotifyDropGuard { notify })
    }

    fn set_revoked(&self) {
        self.revoked.store(true, Ordering::Release);
        self.notify.notify_waiters();
    }

    fn is_revoked(&self) -> bool {
        self.revoked.load(Ordering::Acquire)
    }
}

struct LeaseHandleInner {
    /// Notify is updated when lease_released is changed.
    worker_tx: UnboundedSender<LeaseWorkerMessage>,
    lease_revoked_notify: Arc<LeaseRevokedNotify>,
    id: i64,
}

impl LeaseHandleInner {
    fn new(
        id: i64,
        worker_tx: UnboundedSender<LeaseWorkerMessage>,
        lease_revoked_notify: Arc<LeaseRevokedNotify>,
    ) -> Self {
        Self {
            id,
            worker_tx,
            lease_revoked_notify,
        }
    }
}

impl Drop for LeaseHandleInner {
    fn drop(&mut self) {
        self.worker_tx
            .send(LeaseWorkerMessage::RevokeLease { lease_id: self.id })
            .ok();
    }
}

/// A lease handle represents a held etcd lease.
///
/// The lease handle can be cloned, but when all copies of the lease handle are dropped, then the lease will be
/// revoked on the server.
///
/// However, there is a chance that the lease may be revoked if this client has difficulty issuing a lease keep-alive
/// request before the lease expires. The [`LeaseHandle::monitor`] method is provided to check for this so you
/// can act accordingly in your program.
#[derive(Clone)]
pub struct LeaseHandle {
    inner: Arc<LeaseHandleInner>,
}

pub(crate) struct LeaseWorkerHandle {
    worker_tx: UnboundedSender<LeaseWorkerMessage>,
}

impl LeaseWorkerHandle {
    fn manage_lease(self, lease_id: i64, expires_at: Instant) -> Arc<LeaseHandleInner> {
        let (notify, drop_guard) = LeaseRevokedNotify::new();
        self.worker_tx
            .send(LeaseWorkerMessage::ManageLease {
                lease_id,
                drop_guard,
                expires_at,
            })
            .ok();

        Arc::new(LeaseHandleInner::new(lease_id, self.worker_tx, notify))
    }
}

pub enum LeaseWorkerMessage {
    ManageLease {
        lease_id: i64,
        drop_guard: LeaseRevokedNotifyDropGuard,
        expires_at: Instant,
    },
    RevokeLease {
        lease_id: i64,
    },
}

impl LeaseHandle {
    pub(crate) async fn grant(
        mut lease_client: LeaseClient<AuthedChannel>,
        worker_handle: LeaseWorkerHandle,
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

        let inner = worker_handle.manage_lease(
            lease.id,
            Instant::now() + Duration::from_secs(lease.ttl as _),
        );
        Ok(LeaseHandle { inner })
    }

    /// Returns the lease id, or None if the lease has expired.
    pub fn lease_id(&self) -> Option<i64> {
        self.inner
            .lease_revoked_notify
            .is_revoked()
            .then_some(self.inner.id)
    }

    /// Monitors the lease, resolving if the lease for whatever reason was revoked or expired by the server
    /// (and thus should not be used anymore).
    pub async fn monitor(&self) {
        loop {
            if self.inner.lease_revoked_notify.is_revoked() {
                break;
            }

            self.inner.lease_revoked_notify.notify.notified().await;
        }
    }
}

// fixme: dedupe?
type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;

enum LeaseStreamState {
    Disconnected,
    Connecting(
        BoxFuture<(
            Result<Response<Streaming<LeaseKeepAliveRequest>>, Status>,
            UnboundedSender<LeaseKeepAliveRequest>,
        )>,
    ),
    Reconnecting(Pin<Box<Sleep>>),
    Connected {
        stream: Streaming<LeaseKeepAliveResponse>,
        out: UnboundedSender<LeaseKeepAliveRequest>,
    },
}

struct LeaseStream {
    state: LeaseStreamState,
    lease_client: LeaseClient<AuthedChannel>,
    leases: HashMap<i64, LeaseState>, // fixme: use fast hasher.
    dq: DelayQueue<i64>,
}

struct LeaseState {
    expires_at: Instant,
    notify_drop_guard: LeaseRevokedNotifyDropGuard,
    key: Key,
}

struct LeaseWorker {
    rx: UnboundedReceiver<LeaseWorkerMessage>,
    revoke_join_set: JoinSet<()>,
    stream: LeaseStream,
}

impl LeaseWorker {
    async fn drain_revoke_requests(&mut self) {
        while !self.revoke_join_set.is_empty() {
            let _ = self.revoke_join_set.join_next().await;
        }
    }

    async fn run(mut self) {
        loop {
            enum Action {
                WorkerMessage(Option<LeaseWorkerMessage>),
            }

            let action = tokio::select! {
                message = self.rx.recv() => Action::WorkerMessage(message),
                _ = self.revoke_join_set.join_next(), if !self.revoke_join_set.is_empty() => continue,
            };

            match action {
                Action::WorkerMessage(Some(worker_message)) => {
                    self.handle_worker_message(worker_message);
                }
                Action::WorkerMessage(None) => {
                    self.revoke_all_leases();
                    self.drain_revoke_requests().await;
                    break;
                }
            }
        }
    }

    fn handle_worker_message(&self, worker_message: LeaseWorkerMessage) {
        todo!()
    }

    fn revoke_all_leases(&mut self) {
        let mut leases_to_revoke = Vec::with_capacity(self.stream.leases.len());
        for (lease_id, lease_state) in self.stream.leases.drain() {
            self.stream.dq.remove(&lease_state.key);
            leases_to_revoke.push(lease_id);
        }
    }

    fn spawn_revoke_lease(&self, lease_id: i64) {}

    // async fn run(mut self) {
    //     loop {
    //         match &mut self.state {
    //             LeaseStreamState::Disconnected => {
    //                 let (tx, rx) = unbounded_channel();
    //                 tx.send(LeaseKeepAliveRequest {
    //                     id: self.handle_inner.id,
    //                 })
    //                 .ok();

    //                 let keep_alive_result = tokio::select! {
    //                     res = self.lease_client.lease_keep_alive(UnboundedReceiverStream::new(rx)) => res,
    //                     _ = &mut self.dropped_receiver => {
    //                         self.revoke_lease().await;
    //                         break;
    //                     }
    //                 };
    //                 match keep_alive_result {
    //                     Ok(response) => {
    //                         self.state = LeaseStreamState::HasStream {
    //                             stream: response.into_inner(),
    //                             out: tx,
    //                         };
    //                     }
    //                     Err(error) => {
    //                         tracing::error!(
    //                             "error establishing lease channel for lease: {}, error: {:?}",
    //                             self.handle_inner.id,
    //                             error
    //                         );
    //                         sleep(Duration::from_secs(3)).await;
    //                     }
    //                 }
    //             }
    //             LeaseStreamState::HasStream { stream, out } => {
    //                 enum Action {
    //                     /// All copies of the handle have dropped, so we can go ahead and revoke the lease.
    //                     HandleDropped,
    //                     /// Lease renewal interval has elapsed, so we should send a lease keep alive.
    //                     RequestKeepAlive,
    //                     /// We got a keep-alive response from the server.
    //                     KeepAliveResponse(Option<Result<LeaseKeepAliveResponse, Status>>),
    //                 }

    //                 let action = tokio::select! {
    //                     _ = self.interval.tick() => Action::RequestKeepAlive,
    //                     _ = &mut self.dropped_receiver => Action::HandleDropped,
    //                     res = stream.next() => Action::KeepAliveResponse(res),
    //                 };

    //                 match action {
    //                     Action::HandleDropped => {
    //                         self.revoke_lease().await;
    //                         break;
    //                     }
    //                     Action::RequestKeepAlive => {
    //                         out.send(LeaseKeepAliveRequest {
    //                             id: self.handle_inner.id,
    //                         })
    //                         .ok();
    //                     }
    //                     Action::KeepAliveResponse(None) => {
    //                         tracing::info!(
    //                             "lease {} keep-alive channel closed, re-establishing",
    //                             self.handle_inner.id
    //                         );
    //                         self.state = LeaseStreamState::Disconnected;
    //                     }
    //                     Action::KeepAliveResponse(Some(Err(status))) => {
    //                         tracing::error!(
    //                             "error renewing lease {}, keep-alive channel returned error: {:?}, will retry",
    //                             self.handle_inner.id,
    //                             status
    //                         );
    //                         self.interval.reset_after(Duration::from_secs(1));
    //                     }
    //                     Action::KeepAliveResponse(Some(Ok(response))) => {
    //                         assert_eq!(
    //                             response.id, self.handle_inner.id,
    //                             "bug: lease id does not match"
    //                         );

    //                         if response.ttl > 0 {
    //                             tracing::info!(
    //                                 "lease {} has been kept-alive, new TTL: {}",
    //                                 self.handle_inner.id,
    //                                 response.ttl
    //                             );
    //                             self.interval.reset_after(Duration::from_millis(
    //                                 (((response.ttl * 1000) as f32) * 0.5) as _,
    //                             ));
    //                             self.timeout_at =
    //                                 Instant::now() + Duration::from_secs(response.ttl as _);
    //                         } else {
    //                             tracing::error!(
    //                                 "lease {} has expired before we could keep it alive",
    //                                 self.handle_inner.id
    //                             );
    //                             self.state = LeaseStreamState::Revoked;
    //                             break;
    //                         }
    //                     }
    //                 }
    //             }
    //             LeaseStreamState::Revoked => {
    //                 self.handle_inner.set_revoked();
    //                 break;
    //             }
    //         }
    //     }
    // }

    // async fn revoke_lease(mut self) {
    //     let revoke_future = async {
    //         loop {
    //             match self
    //                 .lease_client
    //                 .lease_revoke(LeaseRevokeRequest {
    //                     id: self.handle_inner.id,
    //                 })
    //                 .await
    //             {
    //                 Ok(res) => {
    //                     break res;
    //                 }
    //                 Err(_) => {
    //                     sleep(Duration::from_secs(1)).await;
    //                 }
    //             }
    //         }
    //     };

    //     match timeout_at(self.timeout_at, revoke_future).await {
    //         Ok(_) => {
    //             tracing::warn!("revoked lease {} successfully.", self.handle_inner.id);
    //         }
    //         Err(_) => {
    //             tracing::warn!(
    //                 "timed out trying to issue revoke lease {} call before the lease expired.",
    //                 self.handle_inner.id
    //             );
    //         }
    //     }
    // }
}
