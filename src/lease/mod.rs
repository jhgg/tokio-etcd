use std::{
    collections::HashMap,
    future::{pending, Future},
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use futures::Stream;
use tokio::{
    sync::{
        mpsc::{UnboundedReceiver, UnboundedSender},
        Notify,
    },
    task::JoinSet,
    time::{timeout_at, Instant, Sleep},
};
use tokio_etcd_grpc_client::{
    AuthedChannel, LeaseClient, LeaseGrantRequest, LeaseKeepAliveRequest, LeaseKeepAliveResponse,
    LeaseRevokeRequest,
};
use tokio_timer::{delay_queue::Key, DelayQueue};
use tonic::{Response, Status, Streaming};

use crate::{ids::IdFastHasherBuilder, utils::backoff::ExponentialBackoff, LeaseId};

struct LeaseRevokedNotify {
    notify: Notify,
    revoked: AtomicBool,
}

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

/// Holds a [`LeaseRevokedNotify`] and sets it as revoked if for some reason this
/// struct is dropped, which might imply that the message was dropped, or that
/// the worker panicked.
struct LeaseRevokedNotifyDropGuard {
    notify: Arc<LeaseRevokedNotify>,
}

impl Drop for LeaseRevokedNotifyDropGuard {
    fn drop(&mut self) {
        self.notify.set_revoked();
    }
}

struct LeaseHandleInner {
    worker_tx: UnboundedSender<LeaseWorkerMessage>,
    notify: Arc<LeaseRevokedNotify>,
    id: LeaseId,
}

impl LeaseHandleInner {
    fn new(
        id: LeaseId,
        worker_tx: UnboundedSender<LeaseWorkerMessage>,
        notify: Arc<LeaseRevokedNotify>,
    ) -> Self {
        Self {
            id,
            worker_tx,
            notify,
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

#[derive(Clone)]
pub(crate) struct LeaseWorkerHandle {
    worker_tx: UnboundedSender<LeaseWorkerMessage>,
}

impl LeaseWorkerHandle {
    fn keep_alive_lease(self, lease_id: LeaseId, expires_at: Instant) -> Arc<LeaseHandleInner> {
        let (notify, drop_guard) = LeaseRevokedNotify::new();
        self.worker_tx
            .send(LeaseWorkerMessage::KeepAliveLease {
                lease_id,
                drop_guard,
                expires_at,
            })
            .ok();

        Arc::new(LeaseHandleInner::new(lease_id, self.worker_tx, notify))
    }
}

pub enum LeaseWorkerMessage {
    KeepAliveLease {
        lease_id: LeaseId,
        drop_guard: LeaseRevokedNotifyDropGuard,
        expires_at: Instant,
    },
    RevokeLease {
        lease_id: LeaseId,
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

        let lease_id = LeaseId::new(lease.id).expect("invariant: lease id was valid");
        let inner = worker_handle.keep_alive_lease(
            lease_id,
            Instant::now() + Duration::from_secs(lease.ttl as _),
        );
        Ok(LeaseHandle { inner })
    }

    /// Returns the lease id, or None if the lease has expired.
    pub fn lease_id(&self) -> Option<LeaseId> {
        self.inner.notify.is_revoked().then_some(self.inner.id)
    }

    /// Monitors the lease, returning if the lease for whatever reason was revoked or expired by the server
    /// (and thus should not be used anymore).
    pub async fn monitor(&self) {
        loop {
            if self.inner.notify.is_revoked() {
                break;
            }

            self.inner.notify.notify.notified().await;
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
    leases: HashMap<LeaseId, LeaseState, IdFastHasherBuilder>,
    dq: DelayQueue<LeaseId>,
}

impl LeaseStream {
    fn keep_alive_lease(
        &mut self,
        lease_id: LeaseId,
        drop_guard: LeaseRevokedNotifyDropGuard,
        expires_at: Instant,
    ) {
        let key = self
            .dq
            .insert(lease_id, calculate_keepalive_at(expires_at, Instant::now()));
        let state = LeaseState {
            key: Some(key),
            expires_at,
            drop_guard,
        };
        self.leases
            .insert(lease_id, state)
            .expect("invariant: should not have recycled lease id");
    }

    fn cancel_keep_alive(
        &mut self,
        lease_id: LeaseId,
    ) -> Option<(Instant, LeaseRevokedNotifyDropGuard)> {
        let state = self.leases.remove(&lease_id)?;
        if let Some(key) = state.key {
            self.dq.remove(&key);
        }

        Some((state.expires_at, state.drop_guard))
    }

    fn cancel_all_keepalives(&mut self) -> Vec<(LeaseId, Instant, LeaseRevokedNotifyDropGuard)> {
        let mut lease_infos = Vec::with_capacity(self.leases.len());
        for (lease_id, state) in self.leases.drain() {
            if let Some(key) = state.key {
                self.dq.remove(&key);
            }
            lease_infos.push((lease_id, state.expires_at, state.drop_guard));
        }

        lease_infos
    }

    // async fn poll_lease(&mut self) -> LeaseId {
    //     if self.dq.is_empty() {
    //         pending().await;
    //     }

    //     let n = self.dq.next();
    // }
}

struct LeaseState {
    expires_at: Instant,
    drop_guard: LeaseRevokedNotifyDropGuard,
    key: Option<Key>,
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
                    let infos = self.stream.cancel_all_keepalives();
                    for (lease_id, expires_at, drop_guard) in infos {
                        self.spawn_revoke_lease(lease_id, expires_at, drop_guard);
                    }
                    self.drain_revoke_requests().await;
                    break;
                }
            }
        }
    }

    fn handle_worker_message(&mut self, worker_message: LeaseWorkerMessage) {
        match worker_message {
            LeaseWorkerMessage::KeepAliveLease {
                lease_id,
                drop_guard,
                expires_at,
            } => self
                .stream
                .keep_alive_lease(lease_id, drop_guard, expires_at),
            LeaseWorkerMessage::RevokeLease { lease_id } => {
                if let Some((expires_at, drop_guard)) = self.stream.cancel_keep_alive(lease_id) {
                    self.spawn_revoke_lease(lease_id, expires_at, drop_guard);
                }
            }
        }
    }

    fn spawn_revoke_lease(
        &mut self,
        lease_id: LeaseId,
        expires_at: Instant,
        drop_guard: LeaseRevokedNotifyDropGuard,
    ) {
        let mut lease_client = self.stream.lease_client.clone();
        self.revoke_join_set.spawn(async move {
            let revoke_future = async {
                let mut backoff =
                    ExponentialBackoff::new(Duration::from_secs(1), Duration::from_secs(5));
                loop {
                    match lease_client
                        .lease_revoke(LeaseRevokeRequest {
                            id: lease_id.get_i64(),
                        })
                        .await
                    {
                        Ok(res) => {
                            break res;
                        }
                        Err(_) => {
                            backoff.delay().await;
                        }
                    }
                }
            };

            match timeout_at(expires_at, revoke_future).await {
                Ok(_) => {
                    tracing::warn!("revoked lease {} successfully.", lease_id);
                }
                Err(_) => {
                    tracing::warn!(
                        "timed out trying to issue revoke lease {} call before the lease expired.",
                        lease_id
                    );
                }
            }

            // Retain the drop guard till it's explicitly dropped here, to notify when we're sure
            // the lease is revoked.
            drop(drop_guard);
        });
    }

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
}

fn calculate_keepalive_at(expires_at: Instant, now: Instant) -> Duration {
    let duration_until_expiry = expires_at.saturating_duration_since(now);
    let sixty_six_percent_duration = duration_until_expiry / 3 * 2;
    sixty_six_percent_duration
}
