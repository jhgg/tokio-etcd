use std::{
    collections::{hash_map::Entry, HashMap, VecDeque},
    future::{pending, Future},
    panic,
    pin::Pin,
    sync::Arc,
    time::Duration,
};

use tokio::{
    sync::{
        broadcast::{self, error::RecvError},
        mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender, WeakUnboundedSender},
    },
    task::JoinSet,
    time::{Interval, Sleep},
};
use tokio_etcd_grpc_client::{
    watch_request, AuthedChannel, Event, EventType, KvClient, RangeRequest, RangeResponse,
    WatchClient, WatchProgressRequest, WatchRequest, WatchResponse, PROGRESS_WATCH_ID,
};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::{Response, Status, Streaming};

#[derive(Clone)]
pub struct Watcher {
    tx: UnboundedSender<WorkerMessage>,
}

impl Watcher {
    pub(crate) fn new(
        watch_client: WatchClient<AuthedChannel>,
        kv_client: KvClient<AuthedChannel>,
    ) -> Self {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

        let worker = WatcherWorker::new(rx, tx.downgrade(), watch_client, kv_client);
        tokio::spawn(worker.run());

        Watcher { tx }
    }

    pub async fn watch(&self, key: WatcherKey) -> Result<Watched, WatchError> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.tx
            .send(WorkerMessage::WatchKey { key, sender: tx })
            .ok();

        // fixme: ???
        rx.await.expect("invariant: worker always sends a response")
    }
}

enum WorkerMessage {
    WatchKey {
        key: WatcherKey,
        sender: InitialWatchSender,
    },
    ReceiverDropped(WatchId),
}

// todo: better debug impl?
#[derive(Clone, Debug)]
pub enum WatcherValue {
    Set {
        value: Arc<[u8]>,
        mod_revision: i64,
        create_revision: i64,
    },
    Unset {
        /// The revision of the key that was deleted, if it is known.
        ///
        /// If the key was deleted before we started watching, we won't have the revision of the key that was deleted.
        mod_revision: Option<i64>,
    },
}

// todo: name this better.
#[derive(Debug)]
pub struct Watched {
    pub value: WatcherValue,
    pub receiver: WatcherReceiver,
}

impl Watched {
    pub fn watch_id(&self) -> WatchId {
        self.receiver.watch_id()
    }
}

#[derive(Debug)]
struct WatchReceiverDropGuard {
    tx: UnboundedSender<WorkerMessage>,
    watch_id: WatchId,
}

pub struct WatcherReceiver {
    receiver: broadcast::Receiver<WatcherValue>,
    // this field must be the last field in the struct, as it must
    // be dropped after the receiver.
    watch_receiver_drop_guard: WatchReceiverDropGuard,
}

impl std::fmt::Debug for WatcherReceiver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WatcherReceiver")
            .field("watch_id", &self.watch_id())
            .finish()
    }
}

impl WatcherReceiver {
    pub async fn recv(&mut self) -> WatcherValue {
        loop {
            match self.receiver.recv().await {
                Ok(value) => return value,
                // If we have lagged, we'll skip over it and try to receive the next value.
                Err(RecvError::Lagged(_)) => continue,
                Err(RecvError::Closed) => panic!("invariant: worker always sends a value, and never closes the sender unless the receiver is dropped."),
            }
        }
    }

    pub async fn recv_raw(&mut self) -> Result<WatcherValue, RecvError> {
        self.receiver.recv().await
    }

    pub fn watch_id(&self) -> WatchId {
        self.watch_receiver_drop_guard.watch_id
    }

    fn new(
        receiver: broadcast::Receiver<WatcherValue>,
        tx: UnboundedSender<WorkerMessage>,
        watch_id: WatchId,
    ) -> Self {
        Self {
            receiver,
            watch_receiver_drop_guard: WatchReceiverDropGuard { tx, watch_id },
        }
    }
}

impl Drop for WatchReceiverDropGuard {
    fn drop(&mut self) {
        self.tx
            .send(WorkerMessage::ReceiverDropped(self.watch_id))
            .ok();
    }
}

// todo: thiserror.
#[derive(Debug)]
pub enum WatchError {
    EtcdError(Status),
}

type InitialWatchSender = tokio::sync::oneshot::Sender<Result<Watched, WatchError>>;

struct WatcherWorker {
    rx: UnboundedReceiver<WorkerMessage>,
    weak_tx: WeakUnboundedSender<WorkerMessage>,
    // todo: tinyvec?
    in_progress_reads: HashMap<WatcherKey, Vec<InitialWatchSender>>,
    streaming_watcher: StreamingWatcher,
    kv_client: KvClient<AuthedChannel>,
    read_join_set: JoinSet<(WatcherKey, Result<Response<RangeResponse>, Status>)>,
}

type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;

enum StreamingWatcherState {
    Disconnected,
    Connecting(
        BoxFuture<(
            Result<Response<Streaming<WatchResponse>>, Status>,
            UnboundedSender<WatchRequest>,
        )>,
    ),
    Reconnecting(Pin<Box<Sleep>>),
    Connected(ConnectedWatcherStream),
}
impl StreamingWatcherState {
    fn is_disconnected(&self) -> bool {
        matches!(self, Self::Disconnected)
    }
    fn connected(&mut self) -> Option<&mut ConnectedWatcherStream> {
        match self {
            Self::Connected(connected) => Some(connected),
            _ => None,
        }
    }
}

struct StreamingWatcher {
    watch_client: WatchClient<AuthedChannel>,
    state: StreamingWatcherState,
    set: WatcherSet,
    progress_request_interval: Duration,
    connection_incarnation: u64,
}

struct ConnectedWatcherStream {
    stream: Streaming<WatchResponse>,
    sender: UnboundedSender<WatchRequest>,
    progress_request_interval: Pin<Box<Interval>>,
    progress_notifications_requested_without_response: u8,
}

fn progress_request() -> WatchRequest {
    WatchRequest {
        request_union: Some(watch_request::RequestUnion::ProgressRequest(
            WatchProgressRequest {},
        )),
    }
}

#[derive(Debug)]
enum DisconnectReason {
    ProgressTimeout,
    StreamEnded,
    SenderClosed,
}

impl ConnectedWatcherStream {
    const MAX_PROGRESS_NOTIFICATIONS_WITHOUT_RESPONSE: u8 = 3;

    fn new(
        stream: Streaming<WatchResponse>,
        progress_request_interval: Duration,
        sender: UnboundedSender<WatchRequest>,
    ) -> Self {
        ConnectedWatcherStream {
            stream,
            sender,
            progress_request_interval: Box::pin({
                let mut int = tokio::time::interval_at(
                    tokio::time::Instant::now() + progress_request_interval,
                    progress_request_interval,
                );
                int.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                int
            }),
            progress_notifications_requested_without_response: 0,
        }
    }

    fn send(&mut self, request: WatchRequest) {
        self.sender.send(request).ok();
    }

    async fn next_message(&mut self) -> Result<WatchResponse, DisconnectReason> {
        loop {
            let message = tokio::select! {
                message = self.stream.message() => message,
                _ = self.sender.closed() => {
                    return Err(DisconnectReason::SenderClosed);
                }
                _ = self.progress_request_interval.tick() => {
                    // The rationale behind this is that etcd is a bit weird, and if a single watcher is un-synced, we
                    // won't get a progress notification. So, we need to keep sending progress requests until we get a
                    // response. If we don't get a response after a certain number of requests, we should consider the
                    // connection dead (or perhaps even the etcd server malfunctioning, since it's got unsynced watchers
                    // for an extended period).
                    if self.progress_notifications_requested_without_response < Self::MAX_PROGRESS_NOTIFICATIONS_WITHOUT_RESPONSE {
                        self.send(progress_request());
                        self.progress_notifications_requested_without_response += 1;
                        continue;
                    } else {
                        return Err(DisconnectReason::ProgressTimeout);
                    }
                }
            };

            match message {
                Ok(Some(response)) => {
                    if response.watch_id == PROGRESS_WATCH_ID {
                        self.progress_notifications_requested_without_response = 0;
                    }

                    return Ok(response);
                }
                Ok(None) => {
                    return Err(DisconnectReason::StreamEnded);
                }
                Err(_e) => {
                    // todo: log this error?
                    continue;
                }
            };
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
// todo: fast hashing impl for this?
pub struct WatchId(i64);

impl WatchId {
    pub fn into_inner(self) -> i64 {
        self.0
    }

    fn next(self) -> Self {
        WatchId(self.0 + 1)
    }

    fn cancel_request(&self) -> WatchRequest {
        WatchRequest {
            request_union: Some(watch_request::RequestUnion::CancelRequest(
                tokio_etcd_grpc_client::WatchCancelRequest { watch_id: self.0 },
            )),
        }
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum WatcherKey {
    Key(Vec<u8>),
    // fixme: we should have a better way to represent a watcher.
    // Prefix(String),
}

impl WatcherKey {
    pub fn key_str(key: impl Into<String>) -> Self {
        WatcherKey::Key(key.into().into_bytes())
    }

    fn make_range_request(&self) -> RangeRequest {
        let key = match self {
            WatcherKey::Key(key) => key.clone(),
            // WatcherKey::Prefix(prefix) => prefix.clone().into_bytes(),
        };

        RangeRequest {
            key,
            ..Default::default()
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WatcherSyncState {
    Unsynced,
    Syncing,
    Synced,
}

struct WatcherState {
    id: WatchId,
    key: WatcherKey,
    sync_state: WatcherSyncState,
    value: WatcherValue,
    /// The revision which we last received from the etcd server either by virtue of getting a watch response,
    /// or by a progress notification.
    ///
    /// This value is distinct from the WatcherValue's revision, as it represents the the latest revision from etcd
    /// that we know about, and not the revision of the value itself, which may be older, and unable to be watched
    /// (as it may have been compacted).
    revision: i64,
    sender: broadcast::Sender<WatcherValue>,
}

// A subset of the WatcherState, which contains mutable references to fields that are safe to update.
struct UpdatableWatcherState<'a> {
    sync_state: &'a mut WatcherSyncState,
    value: &'a mut WatcherValue,
    revision: &'a mut i64,
    sender: &'a broadcast::Sender<WatcherValue>,
}

impl UpdatableWatcherState<'_> {
    fn process_event(&mut self, event: Event) {
        let kv = event.kv.as_ref().expect("invariant: kv is always present");
        let value = match event.r#type() {
            EventType::Put => WatcherValue::Set {
                value: kv.value.clone().into(),
                mod_revision: kv.mod_revision,
                create_revision: kv.create_revision,
            },
            EventType::Delete => WatcherValue::Unset {
                mod_revision: Some(kv.mod_revision),
            },
        };

        *self.value = value.clone();
        *self.revision = kv.mod_revision.max(*self.revision);
        self.sender.send(value).ok();
    }
}

impl WatcherState {
    fn initial_watch_request(&self) -> WatchRequest {
        let (key, range_end) = match &self.key {
            WatcherKey::Key(key) => (key.clone(), vec![]),
            // WatcherKey::Prefix(prefix) => (prefix.clone(), vec![]),
        };
        WatchRequest {
            request_union: Some(watch_request::RequestUnion::CreateRequest(
                tokio_etcd_grpc_client::WatchCreateRequest {
                    key,
                    range_end,
                    start_revision: self.revision + 1,
                    progress_notify: false,
                    filters: vec![],
                    prev_kv: false,
                    watch_id: self.id.0,
                    fragment: true,
                },
            )),
        }
    }
}

struct WatcherSet {
    next_watch_id: WatchId,
    key_to_watch_id: HashMap<WatcherKey, WatchId>,
    states: HashMap<WatchId, WatcherState>,
    unsynced_watchers: VecDeque<WatchId>,
    syncing_watchers: VecDeque<WatchId>,
    pending_cancels: VecDeque<WatchId>,
    fragmented_responses: HashMap<WatchId, WatchResponse>,
    concurrent_sync_limit: usize,
    broadcast_channel_capacity: usize,
}

impl WatcherSet {
    fn new(concurrent_sync_limit: usize, broadcast_channel_capacity: usize) -> Self {
        WatcherSet {
            next_watch_id: WatchId(0),
            key_to_watch_id: Default::default(),
            states: Default::default(),
            unsynced_watchers: Default::default(),
            syncing_watchers: Default::default(),
            fragmented_responses: Default::default(),
            pending_cancels: Default::default(),
            concurrent_sync_limit,
            broadcast_channel_capacity,
        }
    }

    fn is_empty(&self) -> bool {
        self.states.is_empty()
    }

    fn len(&self) -> usize {
        self.states.len()
    }

    /// Adds a watcher to the set.
    ///
    /// Returns a result, where Ok(InternalWatcherId) is returned if the watcher was added successfully,
    /// and Err(InternalWatcherId) is returned if a watcher with the same key already exists.
    fn add_unsynced_watcher(
        &mut self,
        key: WatcherKey,
        value: WatcherValue,
        revision: i64,
    ) -> Result<WatchId, WatchId> {
        match self.key_to_watch_id.entry(key) {
            Entry::Occupied(ent) => Err(*ent.get()),
            Entry::Vacant(ent) => {
                let watch_id = self.next_watch_id;
                self.next_watch_id = watch_id.next();

                self.states.insert(
                    watch_id,
                    WatcherState {
                        key: ent.key().clone(),
                        sync_state: WatcherSyncState::Unsynced,
                        revision,
                        value,
                        id: watch_id,
                        sender: broadcast::channel(self.broadcast_channel_capacity).0,
                    },
                );
                self.unsynced_watchers.push_back(watch_id);
                ent.insert(watch_id);

                Ok(watch_id)
            }
        }
    }

    /// Returns the next watch request to send to the etcd server in order to sync the connection.
    ///
    /// This function will try to send unsynced watchers first, limiting the number of in-flight requests to sync
    /// watchers depending on config.
    ///
    /// Otherwise, it will send cancel requests for watchers that have been cancelled.
    fn next_watch_request_to_send(&mut self) -> Option<WatchRequest> {
        // If we have watchers that are unsynced, we'll try to sync them first.
        if self.syncing_watchers.len() < self.concurrent_sync_limit {
            if let Some(unsynced_id) = self.unsynced_watchers.front() {
                let state = self
                    .update_watcher(*unsynced_id, |state| {
                        assert!(matches!(state.sync_state, WatcherSyncState::Unsynced));
                        *state.sync_state = WatcherSyncState::Syncing;
                    })
                    .expect("invariant: found unsynced watcher, but watcher not found");
                return Some(state.initial_watch_request());
            }
        }

        // Otherwise, let's send any pending cancel requests.
        self.pending_cancels.pop_front().map(|w| w.cancel_request())
    }

    /// Cancels a watcher by its watch id,
    ///
    /// If `enqueue_pending` is true, the watcher will be enqueued for a cancel request to be sent to the
    /// etcd server. If false, the watcher will be immediately removed, and no cancel request will be sent. Set
    /// to false when you receive a cancel response from the server, and true when you receive a cancel request
    /// from the user.
    fn cancel_watcher(&mut self, watch_id: WatchId, enqueue_pending: bool) -> Option<WatcherKey> {
        let WatcherState {
            key, sync_state, ..
        } = self.states.remove(&watch_id)?;

        self.key_to_watch_id
            .remove(&key)
            .expect("invariant: found key for watch id, but key not found");

        self.fragmented_responses.remove(&watch_id);

        match sync_state {
            WatcherSyncState::Unsynced => {
                self.unsynced_watchers.retain(|id| *id != watch_id);
            }
            WatcherSyncState::Syncing => {
                self.syncing_watchers.retain(|id| *id != watch_id);
            }
            WatcherSyncState::Synced => {}
        }

        // If we're not in the unsynced state, it means that we've already sent a watch request
        // to the etcd server, and we should send a cancel request. Otherwise, we can just remove
        // the watcher from the set, as the server never knew about it.
        if sync_state != WatcherSyncState::Unsynced && enqueue_pending {
            self.pending_cancels.push_back(watch_id);
        }

        Some(key)
    }

    fn update_watcher(
        &mut self,
        watch_id: WatchId,
        f: impl FnOnce(UpdatableWatcherState),
    ) -> Option<&WatcherState> {
        let state = self.states.get_mut(&watch_id)?;
        let prev_sync_state = state.sync_state;
        let updatable_state = UpdatableWatcherState {
            sync_state: &mut state.sync_state,
            value: &mut state.value,
            revision: &mut state.revision,
            sender: &state.sender,
        };
        f(updatable_state);

        use WatcherSyncState::*;
        if prev_sync_state != state.sync_state {
            let mut container_for_state =
                |s: &WatcherSyncState, f: fn(&mut VecDeque<WatchId>, &WatchId)| match s {
                    Unsynced => f(&mut self.unsynced_watchers, &watch_id),
                    Syncing => f(&mut self.syncing_watchers, &watch_id),
                    Synced => {}
                };

            container_for_state(&prev_sync_state, |target, id| {
                // Check if the watcher is at the front of the vecdeq, and if so, we can just pop it.
                // Generally, this should be the case, as we'll be processing watchers in order.
                if target.front() == Some(id) {
                    target.pop_front();
                    return;
                }

                // Otherwise, we'll need to find the item in the vecdeq, and remove it.
                target.retain(|i| i != id);
            });
            container_for_state(&state.sync_state, |target, id| target.push_back(*id));
        }

        Some(state)
    }

    /// Resets the set for a new connection, which ultimately means:
    ///
    /// - Resetting all the sync states to unsynced.
    /// - Clearing all fragmented responses.
    /// - Clearing all pending cancels, since we won't re-start the watchers that were cancelled on the new connection.
    fn reset_for_new_connection(&mut self) {
        for state in self.states.values_mut() {
            state.sync_state = WatcherSyncState::Unsynced;
        }
        self.syncing_watchers.clear();
        self.unsynced_watchers.clear();
        self.unsynced_watchers.extend(self.states.keys());
        self.fragmented_responses.clear();
        self.pending_cancels.clear();
    }

    /// Watch responses can be fragmented, so we'll need to merge them together before we can process them.
    ///
    /// Returns:
    ///  - Some(WatchResponse): if the response was a complete response, and we've merged all the fragments.
    ///  - None: if the response was a fragment, and we're waiting for more fragments.
    fn try_merge_fragmented_response(&mut self, response: WatchResponse) -> Option<WatchResponse> {
        let watch_id = WatchId(response.watch_id);

        match (self.fragmented_responses.entry(watch_id), response.fragment) {
            // We have an existing fragment, and this is a fragment, so we'll merge them.
            (Entry::Occupied(mut ent), true) => {
                ent.get_mut().events.extend(response.events);
                None
            }
            // We have a fragment, but this is a complete response, so we'll remove the fragment and return the
            // complete response.
            (Entry::Occupied(ent), false) => {
                let mut existing = ent.remove();
                existing.events.extend(response.events);
                Some(existing)
            }
            // We don't have a fragment, and this is a fragment, so we'll just insert it.
            (Entry::Vacant(ent), true) => {
                ent.insert(response);
                None
            }
            // We don't have a fragment, and this is a complete response, so we'll just return it.
            (Entry::Vacant(_), false) => Some(response),
        }
    }

    fn has_watcher(&self, watch_id: WatchId) -> bool {
        self.states.contains_key(&watch_id)
    }
}

impl StreamingWatcher {
    // When connecting to etcd, we'll only allow a certain number of watchers to sync concurrently, to avoid
    // overwhelming the server.
    const CONCURRENT_SYNC_LIMIT: usize = 5;
    const BROADCAST_CHANNEL_CAPACITY: usize = 10;

    fn new(watch_client: WatchClient<AuthedChannel>, progress_request_interval: Duration) -> Self {
        Self {
            watch_client,
            state: StreamingWatcherState::Disconnected,
            connection_incarnation: 0,
            set: WatcherSet::new(
                Self::CONCURRENT_SYNC_LIMIT,
                Self::BROADCAST_CHANNEL_CAPACITY,
            ),
            progress_request_interval,
        }
    }

    /// This future is cancel safe.
    async fn progress_forwards(&mut self) {
        loop {
            match &mut self.state {
                // When we're disconnected, we'll just stay pending forever, as we'll expect this
                // future to be cancelled by the caller.
                StreamingWatcherState::Disconnected => {
                    return pending().await;
                }
                StreamingWatcherState::Connecting(connecting) => {
                    match connecting.await {
                        (Ok(response), sender) => {
                            let connected = ConnectedWatcherStream::new(
                                response.into_inner(),
                                self.progress_request_interval,
                                sender,
                            );
                            self.state = StreamingWatcherState::Connected(connected);
                            tracing::info!(
                                "connected to etcd successfully, incarnation: {}",
                                self.connection_incarnation
                            );
                        }
                        (Err(error), _) => {
                            // todo: backoff.
                            let reconnect_delay = Duration::from_secs(5);
                            self.state = StreamingWatcherState::Reconnecting(Box::pin(
                                tokio::time::sleep(reconnect_delay),
                            ));
                            tracing::error!(
                                "failed to connect to etcd: {:?}, incarnation: {}. will reconnect in {:?}.",
                                error,
                                self.connection_incarnation,
                                reconnect_delay
                            );
                        }
                    }
                }
                StreamingWatcherState::Reconnecting(sleep) => {
                    sleep.await;
                    self.do_connect();
                }
                StreamingWatcherState::Connected(connected) => {
                    match connected.next_message().await {
                        Ok(response) => {
                            self.handle_watch_response(response);
                        }
                        Err(disconnect_reason) => {
                            // todo: reconnect timeout? backoff?
                            let reconnect_delay = Duration::from_secs(5);
                            self.state = StreamingWatcherState::Reconnecting(Box::pin(
                                tokio::time::sleep(reconnect_delay),
                            ));
                            tracing::warn!(
                                "disconnected from etcd reason: {:?}, incarnation: {}. will reconnect in {:?}",
                                disconnect_reason,
                                self.connection_incarnation,
                                reconnect_delay
                            );
                        }
                    }
                }
            }
        }
    }

    fn handle_watch_response(&mut self, response: WatchResponse) {
        // When receiving a progress notification, we can update the revision for all watchers, so that
        // when we re-sync them, we'll start from a more recent revision, rather than an older one,
        // which might be compacted.
        if response.is_progress_notify() && response.watch_id == PROGRESS_WATCH_ID {
            let revision = response
                .header
                .expect("invariant: header is always present")
                .revision;

            for state in self.set.states.values_mut() {
                state.revision = revision;
            }

            return;
        }

        let watch_id = WatchId(response.watch_id);

        // There is a chance that we may receive a watch response for a watcher we already don't care about.
        // In this case, we'll just ignore the response, since we've already cancelled the watcher.
        if !self.set.has_watcher(watch_id) {
            tracing::info!(
                "received response for unknown watcher: {:?} - ignoring",
                response.watch_id
            );
            return;
        }

        // Handle fragmented responses by merging them together if necessary.
        let Some(response) = self.set.try_merge_fragmented_response(response) else {
            return;
        };

        if response.canceled {
            self.set.cancel_watcher(watch_id, false);
        } else {
            self.set.update_watcher(watch_id, |mut s| {
                if response.compact_revision != 0 {
                    tracing::warn!(
                        "when trying to sync watcher {:?} at revision {}, etcd server returned a compact revision of {}, restarting watcher at compact revision.",
                        watch_id, s.revision, response.compact_revision
                    );
                    *s.sync_state = WatcherSyncState::Unsynced;
                    // fixme: log compact revision properly, we need to re-fetch the entire key potentially?
                    *s.revision = response.compact_revision;
                    return;
                }

                // the server has acknowledged the watcher, so we can mark it as synced.
                if response.created {
                    tracing::info!("watcher {:?} synced (incarnation: {}", watch_id, self.connection_incarnation);
                    *s.sync_state = WatcherSyncState::Synced;
                } else {
                    *s.revision = response
                        .header
                        .expect("invariant: header is always present")
                        .revision;
                }

                for event in response.events {
                    s.process_event(event);
                }
            });
        }

        self.sync_connection();
    }

    fn add_watcher(
        &mut self,
        key: WatcherKey,
        value: WatcherValue,
        revision: i64,
    ) -> Result<&WatcherState, WatchId> {
        let watch_id = self.set.add_unsynced_watcher(key, value, revision)?;
        tracing::info!(
            "added watcher, key: {:?}, watch_id: {:?}",
            self.set.states[&watch_id].key,
            watch_id
        );
        self.sync_connection();

        Ok(&self.set.states[&watch_id])
    }

    fn cancel_watcher(&mut self, watch_id: WatchId) -> Option<WatcherKey> {
        let key = self.set.cancel_watcher(watch_id, true)?;
        tracing::info!(
            "cancelled watcher, key: {:?}, watch_id: {:?}",
            key,
            watch_id
        );
        self.sync_connection();

        Some(key)
    }

    fn get_state_by_key(&self, key: &WatcherKey) -> Option<&WatcherState> {
        let watch_id = self.set.key_to_watch_id.get(key)?;

        Some(
            self.set
                .states
                .get(watch_id)
                .expect("invariant: key exists for watch id"),
        )
    }

    fn sync_connection(&mut self) {
        if self.set.is_empty() {
            // There are no watchers to sync, so we can disconnect.
            if self.state.is_disconnected() {
                return;
            }
            tracing::info!("no watchers to sync, disconnecting");
            self.state = StreamingWatcherState::Disconnected;
        } else if self.state.is_disconnected() {
            // There are watchers to sync, so we should connect.
            self.do_connect();
        } else if let Some(connected) = self.state.connected() {
            while let Some(watch_request) = self.set.next_watch_request_to_send() {
                connected.send(watch_request);
            }
        }
    }

    fn do_connect(&mut self) {
        self.set.reset_for_new_connection();

        if self.set.is_empty() {
            self.state = StreamingWatcherState::Disconnected;
            return;
        }

        self.connection_incarnation += 1;

        tracing::info!(
            "connecting to etcd for new watcher set (incarnation: {}, unsynced: {})",
            self.connection_incarnation,
            self.set.len(),
        );

        // We'll start by constructing all the initial watch requests.
        let (sender, receiver) = unbounded_channel();
        while let Some(watch_request) = self.set.next_watch_request_to_send() {
            sender.send(watch_request).ok();
        }

        // Then we connect, and send the requests.
        let mut watch_client = self.watch_client.clone();
        self.state = StreamingWatcherState::Connecting(Box::pin(async move {
            let res = watch_client
                .watch(UnboundedReceiverStream::new(receiver))
                .await;

            (res, sender)
        }));
    }
}

impl WatcherWorker {
    fn new(
        rx: UnboundedReceiver<WorkerMessage>,
        weak_tx: WeakUnboundedSender<WorkerMessage>,
        watch_client: WatchClient<AuthedChannel>,
        kv_client: KvClient<AuthedChannel>,
    ) -> Self {
        Self {
            rx,
            weak_tx,
            kv_client,
            in_progress_reads: Default::default(),
            streaming_watcher: StreamingWatcher::new(watch_client, Duration::from_secs(60)),
            read_join_set: JoinSet::new(),
        }
    }

    async fn run(mut self) {
        loop {
            enum Action {
                WorkerMessage(WorkerMessage),
                ReadResult((WatcherKey, Result<Response<RangeResponse>, Status>)),
            }

            let action = tokio::select! {
                message = self.rx.recv() => {
                    if let Some(message) = message {
                        Action::WorkerMessage(message)
                    } else {
                        break;
                    }
                },
                Some(read_request_result) = self.read_join_set.join_next(), if !self.read_join_set.is_empty() => {
                    match read_request_result {
                        Ok(response) => Action::ReadResult(response),
                        Err(panic) => panic::resume_unwind(panic.into_panic()),
                    }
                }
                _ = self.streaming_watcher.progress_forwards() => { continue; }
            };

            match action {
                Action::WorkerMessage(message) => self.handle_worker_message(message),
                Action::ReadResult((key, value)) => {
                    self.handle_read_result(key, value);
                }
            }
        }
    }

    fn handle_worker_message(&mut self, worker_message: WorkerMessage) {
        match worker_message {
            WorkerMessage::WatchKey { key, sender } => {
                self.do_watch_key(key, sender);
            }
            WorkerMessage::ReceiverDropped(watch_id) => {
                self.maybe_cancel_watcher(watch_id);
            }
        }
    }

    fn handle_read_result(
        &mut self,
        key: WatcherKey,
        value: Result<Response<RangeResponse>, Status>,
    ) {
        let Some(senders) = self.in_progress_reads.remove(&key) else {
            return;
        };

        let Some(tx) = self.weak_tx.upgrade() else {
            return;
        };

        match value {
            Ok(response) => {
                let response = response.into_inner();
                let kv = response.kvs.into_iter().next();
                let revision = response
                    .header
                    .expect("invariant: header is always present")
                    .revision;

                let value = match kv {
                    Some(kv) => WatcherValue::Set {
                        value: kv.value.into(),
                        mod_revision: kv.mod_revision,
                        create_revision: kv.create_revision,
                    },
                    None => WatcherValue::Unset { mod_revision: None },
                };

                // Now, begin watching:
                let watch_id = {
                    let state = self
                        .streaming_watcher
                        .add_watcher(key.clone(), value, revision)
                        .expect("invariant: the watcher should be new");

                    // fixme: should we log when the sender is closed? or just ignore it?

                    for sender in senders {
                        if sender.is_closed() {
                            continue;
                        }

                        sender
                            .send(Ok(Watched {
                                value: state.value.clone(),
                                receiver: WatcherReceiver::new(
                                    state.sender.subscribe(),
                                    tx.clone(),
                                    state.id,
                                ),
                            }))
                            .ok();
                    }

                    state.id
                };

                // If somehow all senders were dropped, we can detect that here and cancel the watcher.
                self.maybe_cancel_watcher(watch_id);
            }
            Err(status) => {
                for sender in senders {
                    if sender.is_closed() {
                        continue;
                    }

                    sender.send(Err(WatchError::EtcdError(status.clone()))).ok();
                }
            }
        }
    }

    fn do_watch_key(&mut self, key: WatcherKey, sender: InitialWatchSender) {
        // If the sender is closed, we'll just return, since we can't send the result.
        if sender.is_closed() {
            return;
        }

        // We should be able to upgrade if there is a handle to the worker which sent us this request. If not,
        // there's no worker handle, so we'll just return.
        let Some(tx) = self.weak_tx.upgrade() else {
            return;
        };

        // Check to see if we're already watching that key, so we can duplicate the watcher:
        if let Some(state) = self.streaming_watcher.get_state_by_key(&key) {
            // We indeed have the key? Let's just send the initial state to the resolver.
            sender
                .send(Ok(Watched {
                    value: state.value.clone(),
                    receiver: WatcherReceiver::new(state.sender.subscribe(), tx, state.id),
                }))
                .ok();

            return;
        }

        // Otherwise, we'll need to try and fetch the key from etcd, and then add a watcher.
        match self.in_progress_reads.entry(key) {
            Entry::Occupied(mut ent) => ent.get_mut().push(sender),
            Entry::Vacant(ent) => {
                let key = ent.key().clone();
                ent.insert(vec![sender]);

                let mut kv_client = self.kv_client.clone();
                self.read_join_set.spawn(async move {
                    let result = kv_client.range(key.make_range_request()).await;

                    (key, result)
                });
            }
        }
    }

    /// Checks a watcher to see if all receivers have been dropped, and if so, cancels the watcher.
    fn maybe_cancel_watcher(&mut self, watch_id: WatchId) {
        let all_receivers_dropped = self
            .streaming_watcher
            .set
            .states
            .get(&watch_id)
            .map_or(true, |state| state.sender.receiver_count() == 0);

        if all_receivers_dropped {
            self.streaming_watcher.cancel_watcher(watch_id);
        }
    }
}
