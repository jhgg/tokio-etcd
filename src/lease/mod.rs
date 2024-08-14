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

use futures::FutureExt;
use tokio::{
    sync::{
        mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
        Notify,
    },
    task::JoinSet,
    time::{sleep, timeout_at, Instant, Sleep},
};
use tokio_etcd_grpc_client::{
    AuthedChannel, LeaseClient, LeaseGrantRequest, LeaseKeepAliveRequest, LeaseKeepAliveResponse,
    LeaseRevokeRequest,
};
use tokio_stream::{wrappers::UnboundedReceiverStream, StreamExt};
use tokio_util::time::{delay_queue::Key, DelayQueue};
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
pub(crate) struct LeaseRevokedNotifyDropGuard {
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
            .send(LeaseWorkerMessage::RevokeLease { id: self.id })
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
    sender: UnboundedSender<LeaseWorkerMessage>,
}

impl LeaseWorkerHandle {
    fn keep_alive_lease(self, id: LeaseId, expires_at: Instant) -> Arc<LeaseHandleInner> {
        let (notify, drop_guard) = LeaseRevokedNotify::new();
        self.sender
            .send(LeaseWorkerMessage::KeepAliveLease {
                id,
                drop_guard,
                expires_at,
            })
            .ok();

        Arc::new(LeaseHandleInner::new(id, self.sender, notify))
    }

    pub(crate) fn spawn(lease_client: LeaseClient<AuthedChannel>) -> Self {
        let (lease_worker, sender) = LeaseWorker::new(lease_client);
        tokio::spawn(lease_worker.run());
        Self::from_sender(sender)
    }

    pub(crate) fn from_sender(sender: UnboundedSender<LeaseWorkerMessage>) -> Self {
        Self { sender }
    }

    pub(crate) fn into_inner(self) -> UnboundedSender<LeaseWorkerMessage> {
        self.sender
    }
}

pub enum LeaseWorkerMessage {
    KeepAliveLease {
        id: LeaseId,
        drop_guard: LeaseRevokedNotifyDropGuard,
        expires_at: Instant,
    },
    RevokeLease {
        id: LeaseId,
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

        let id = LeaseId::new(lease.id).expect("invariant: lease id was valid");
        let inner = worker_handle
            .keep_alive_lease(id, Instant::now() + Duration::from_secs(lease.ttl as _));
        Ok(LeaseHandle { inner })
    }

    /// Returns the lease id, or None if the lease has expired.
    pub fn id(&self) -> Option<LeaseId> {
        (!self.inner.notify.is_revoked()).then_some(self.inner.id)
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
    Connecting {
        connecting: BoxFuture<Result<Response<Streaming<LeaseKeepAliveResponse>>, Status>>,
        out: UnboundedSender<LeaseKeepAliveRequest>,
    },
    Reconnecting(Pin<Box<Sleep>>),
    Connected {
        stream: Streaming<LeaseKeepAliveResponse>,
        out: UnboundedSender<LeaseKeepAliveRequest>,
    },
}

struct LeaseStream {
    state: LeaseStreamState,
    lease_client: LeaseClient<AuthedChannel>,
    backoff: ExponentialBackoff,
}

enum LeaseStreamMessage {
    Connecting,
    Response(LeaseKeepAliveResponse),
}

impl LeaseStream {
    fn new(lease_client: LeaseClient<AuthedChannel>) -> Self {
        Self {
            state: LeaseStreamState::Disconnected,
            lease_client,
            backoff: ExponentialBackoff::new(Duration::from_secs(1), Duration::from_secs(5)),
        }
    }

    fn can_send(&self) -> bool {
        matches!(
            self.state,
            LeaseStreamState::Connected { .. } | LeaseStreamState::Reconnecting { .. }
        )
    }

    fn send_keep_alive(&mut self, id: LeaseId) -> bool {
        match &mut self.state {
            LeaseStreamState::Connecting { out, .. } | LeaseStreamState::Connected { out, .. } => {
                out.send(LeaseKeepAliveRequest { id: id.get_i64() }).is_ok()
            }
            _ => false,
        }
    }

    async fn next_message(&mut self) -> LeaseStreamMessage {
        loop {
            match &mut self.state {
                LeaseStreamState::Disconnected => pending().await,
                LeaseStreamState::Connecting { connecting, out } => match connecting.await {
                    Ok(streaming) => {
                        self.backoff.succeed();
                        self.state = LeaseStreamState::Connected {
                            stream: streaming.into_inner(),
                            out: out.clone(),
                        };
                    }
                    Err(status) => {
                        let delay = self.backoff.fail();
                        tracing::error!(
                            "failed to connect to lease keep-alive channel, error: {:?}, will reconnect in {:?}",
                            status,
                            delay
                        );
                        self.state = LeaseStreamState::Reconnecting(Box::pin(sleep(delay)));
                    }
                },
                LeaseStreamState::Reconnecting(sleep) => {
                    sleep.await;
                    let (tx, rx) = unbounded_channel();
                    let mut lease_client = self.lease_client.clone();
                    self.state = LeaseStreamState::Connecting {
                        out: tx,
                        connecting: Box::pin(async move {
                            lease_client
                                .lease_keep_alive(UnboundedReceiverStream::new(rx))
                                .await
                        }),
                    };
                    return LeaseStreamMessage::Connecting;
                }
                LeaseStreamState::Connected { stream, .. } => match stream.next().await {
                    Some(Ok(r)) => {
                        return LeaseStreamMessage::Response(r);
                    }
                    Some(Err(status)) => {
                        tracing::error!(
                            "received error from lease keep-alive stream: {:?}",
                            status
                        );
                    }
                    None => {
                        self.state = LeaseStreamState::Disconnected;
                    }
                },
            }
        }
    }

    fn disconnect(&mut self) {
        self.state = LeaseStreamState::Disconnected;
    }

    fn connect(&mut self) {
        if !matches!(self.state, LeaseStreamState::Disconnected) {
            self.state = LeaseStreamState::Reconnecting(Box::pin(sleep(Duration::from_secs(0))));
        }
    }
}

#[derive(Default)]
struct LeaseMap {
    leases: HashMap<LeaseId, LeaseState, IdFastHasherBuilder>,
    dq: DelayQueue<LeaseId>,
}

impl LeaseMap {
    fn is_empty(&self) -> bool {
        self.leases.is_empty()
    }

    fn keep_alive_lease(
        &mut self,
        id: LeaseId,
        drop_guard: LeaseRevokedNotifyDropGuard,
        expires_at: Instant,
    ) {
        let key = self
            .dq
            .insert(id, calculate_keepalive_at(expires_at, Instant::now()));
        let state = LeaseState {
            key: Some(key),
            expires_at,
            drop_guard,
        };
        self.leases
            .insert(id, state)
            .expect("invariant: should not have recycled lease id");
    }

    fn cancel_keep_alive(&mut self, id: LeaseId) -> Option<(Instant, LeaseRevokedNotifyDropGuard)> {
        let state = self.leases.remove(&id)?;
        if let Some(key) = state.key {
            self.dq.remove(&key);
        }

        Some((state.expires_at, state.drop_guard))
    }

    fn cancel_all_keep_alives(&mut self) -> Vec<(LeaseId, Instant, LeaseRevokedNotifyDropGuard)> {
        let mut lease_infos = Vec::with_capacity(self.leases.len());
        for (id, state) in self.leases.drain() {
            if let Some(key) = state.key {
                self.dq.remove(&key);
            }
            lease_infos.push((id, state.expires_at, state.drop_guard));
        }

        lease_infos
    }

    /// Leases that are currently being renewed will have a `None` key. If we've reconnected, we need to
    /// re-schedule all the keep-alives that were sent but have not yet been acknowledged.
    fn ensure_all_keep_alives_scheduled(&mut self) {
        let now = Instant::now();
        for (id, state) in &mut self.leases {
            if state.key.is_none() {
                state.key = Some(
                    self.dq
                        .insert(*id, calculate_keepalive_at(state.expires_at, now)),
                );
            }
        }
    }

    async fn next_lease_to_keep_alive(&mut self) -> LeaseId {
        match self.dq.next().await {
            Some(expired) => {
                let id = expired.into_inner();
                let lease_state = self
                    .leases
                    .get_mut(&id)
                    .expect("invariant: lease should exist");

                lease_state.key = None;
                id
            }
            None => pending().await,
        }
    }

    fn update_lease(&mut self, id: LeaseId, expires_at: Instant) -> Option<()> {
        let lease_state = self.leases.get_mut(&id)?;
        if let Some(key) = &lease_state.key {
            self.dq.remove(key);
        }

        lease_state.expires_at = expires_at;
        lease_state.key = Some(
            self.dq
                .insert(id, calculate_keepalive_at(expires_at, Instant::now())),
        );
        Some(())
    }
}

struct LeaseState {
    expires_at: Instant,
    drop_guard: LeaseRevokedNotifyDropGuard,
    key: Option<Key>,
}

struct LeaseWorker {
    receiver: UnboundedReceiver<LeaseWorkerMessage>,
    revoke_join_set: JoinSet<()>,
    map: LeaseMap,
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
                SendLeaseKeepAlive(LeaseId),
                LeaseStreamMessage(LeaseStreamMessage),
            }

            let action = tokio::select! {
                message = self.receiver.recv() => Action::WorkerMessage(message),
                _ = self.revoke_join_set.join_next(), if !self.revoke_join_set.is_empty() => continue,
                lease_id = self.map.next_lease_to_keep_alive(), if self.stream.can_send() => {
                    Action::SendLeaseKeepAlive(lease_id)
                }
                message = self.stream.next_message() => Action::LeaseStreamMessage(message),
            };

            match action {
                Action::WorkerMessage(Some(worker_message)) => {
                    self.handle_worker_message(worker_message);
                }
                Action::WorkerMessage(None) => {
                    let infos = self.map.cancel_all_keep_alives();
                    for (id, expires_at, drop_guard) in infos {
                        self.spawn_revoke_lease(id, expires_at, drop_guard);
                    }
                    self.drain_revoke_requests().await;
                    break;
                }
                Action::LeaseStreamMessage(message) => {
                    self.handle_lease_stream_message(message);
                }
                Action::SendLeaseKeepAlive(id) => {
                    self.stream.send_keep_alive(id);
                }
            }

            self.sync_connection();
        }
    }

    fn handle_worker_message(&mut self, worker_message: LeaseWorkerMessage) {
        match worker_message {
            LeaseWorkerMessage::KeepAliveLease {
                id,
                drop_guard,
                expires_at,
            } => self.map.keep_alive_lease(id, drop_guard, expires_at),
            LeaseWorkerMessage::RevokeLease { id } => {
                if let Some((expires_at, drop_guard)) = self.map.cancel_keep_alive(id) {
                    self.spawn_revoke_lease(id, expires_at, drop_guard);
                }
            }
        }
    }

    fn spawn_revoke_lease(
        &mut self,
        id: LeaseId,
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
                        .lease_revoke(LeaseRevokeRequest { id: id.get_i64() })
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
                    tracing::warn!("revoked lease {} successfully.", id);
                }
                Err(_) => {
                    tracing::warn!(
                        "timed out trying to issue revoke lease {} call before the lease expired.",
                        id
                    );
                }
            }

            // Retain the drop guard till it's explicitly dropped here, to notify when we're sure
            // the lease is revoked.
            drop(drop_guard);
        });
    }

    fn handle_lease_stream_message(&mut self, message: LeaseStreamMessage) {
        match message {
            LeaseStreamMessage::Connecting => {
                // Re-schedule and send all in-flight keep-alive messages.
                self.map.ensure_all_keep_alives_scheduled();
                while let Some(id) = self.map.next_lease_to_keep_alive().now_or_never() {
                    self.stream.send_keep_alive(id);
                }
            }
            LeaseStreamMessage::Response(response) => {
                let id = LeaseId::new(response.id)
                    .expect("invariant: server sent back invalid lease id");
                if response.ttl <= 0 {
                    self.map.cancel_keep_alive(id);
                } else {
                    let expires_at = Instant::now() + Duration::from_secs(response.ttl as _);
                    self.map.update_lease(id, expires_at);
                }
            }
        }
    }

    fn new(
        lease_client: LeaseClient<AuthedChannel>,
    ) -> (Self, UnboundedSender<LeaseWorkerMessage>) {
        let (sender, receiver) = unbounded_channel();

        let worker = Self {
            receiver,
            stream: LeaseStream::new(lease_client),
            revoke_join_set: Default::default(),
            map: Default::default(),
        };

        (worker, sender)
    }

    fn sync_connection(&mut self) {
        if self.map.is_empty() {
            self.stream.disconnect();
        } else {
            self.stream.connect();
        }
    }
}

fn calculate_keepalive_at(expires_at: Instant, now: Instant) -> Duration {
    let duration_until_expiry = expires_at.saturating_duration_since(now);
    let sixty_six_percent_duration = duration_until_expiry / 3 * 2;
    sixty_six_percent_duration
}
