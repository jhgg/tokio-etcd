use std::{
    collections::{hash_map::Entry, HashMap, VecDeque},
    future::{pending, Future},
    panic,
    pin::Pin,
    sync::Arc,
    time::Duration,
};

use indexmap::IndexSet;
use tokio::{
    sync::{
        broadcast,
        mpsc::{unbounded_channel, Receiver, Sender, UnboundedSender},
    },
    task::JoinSet,
    time::{Interval, Sleep},
};
use tokio_etcd_grpc_client::{
    watch_request, AuthedChannel, Event, EventType, KvClient, RangeRequest, RangeResponse,
    WatchClient, WatchProgressRequest, WatchRequest, WatchResponse,
};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::{Response, Status, Streaming};

#[derive(Clone)]
pub struct Watcher {
    tx: Sender<WorkerMessage>,
}

impl Watcher {
    pub fn new(
        watch_client: WatchClient<AuthedChannel>,
        kv_client: KvClient<AuthedChannel>,
    ) -> Self {
        let (tx, rx) = tokio::sync::mpsc::channel(1);

        let worker = WatcherWorker::new(rx, watch_client, kv_client);
        tokio::spawn(worker.run());

        Watcher { tx }
    }

    pub async fn watch(&self, key: WatcherKey) -> Result<InitialWatchState, WatchError> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.tx
            .send(WorkerMessage::WatchKey { key, sender: tx })
            .await
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
}

// todo: better debug impl?
#[derive(Clone, Debug)]
pub struct WatcherValue {
    pub value: Option<Arc<[u8]>>,
    pub revision: i64,
}

// todo: name this better.
#[derive(Debug)]
pub struct InitialWatchState {
    pub watch_id: WatchId,
    pub value: WatcherValue,
    pub receiver: broadcast::Receiver<WatcherValue>,
}

// todo: thiserror.
#[derive(Debug)]
pub enum WatchError {
    EtcdError(Status),
}

type InitialWatchSender = tokio::sync::oneshot::Sender<Result<InitialWatchState, WatchError>>;

struct WatcherWorker {
    rx: Receiver<WorkerMessage>,
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
}

struct StreamingWatcher {
    watch_client: WatchClient<AuthedChannel>,
    state: StreamingWatcherState,
    set: WatcherSet,
    progress_request_interval: Duration,
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

enum DisconnectReason {
    ProgressTimeout,
    StreamEnded,
    SenderClosed,
}

impl ConnectedWatcherStream {
    const MAX_PROGRESS_NOTIFICATIONS_WITHOUT_RESPONSE: u8 = 3;
    const PROGRESS_WATCH_ID: i64 = -1;

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
                    // todo: update revision for watcher state based on header.revision.
                    if response.watch_id == Self::PROGRESS_WATCH_ID {
                        self.progress_notifications_requested_without_response = 0;
                        continue;
                    }

                    return Ok(response);
                }
                Ok(None) => {
                    return Err(DisconnectReason::StreamEnded);
                }
                Err(e) => {
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
}

#[derive(Clone, PartialEq, Eq, Hash)]
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

#[derive(Clone, Copy, Debug)]
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
    sender: broadcast::Sender<WatcherValue>,
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
                    start_revision: self.value.revision + 1,
                    progress_notify: false,
                    filters: vec![],
                    prev_kv: false,
                    watch_id: self.id.0,
                    fragment: true,
                },
            )),
        }
    }

    fn update_from_event(&mut self, event: Event) {
        let kv = event.kv.as_ref().expect("invariant: kv is always present");
        let value = WatcherValue {
            revision: kv.mod_revision,
            value: match event.r#type() {
                EventType::Put => Some(kv.value.clone().into()),
                EventType::Delete => None,
            },
        };
        self.value = value.clone();
        self.sender.send(value).ok();
    }
}

struct WatcherSet {
    next_watch_id: WatchId,
    key_to_watch_id: HashMap<WatcherKey, WatchId>,
    states: HashMap<WatchId, WatcherState>,
    // fixme: maybe make VecDeque?
    unsynced_watchers: IndexSet<WatchId>,
    syncing_watchers: IndexSet<WatchId>,
    pending_cancels: VecDeque<WatchId>,
    fragmented_responses: HashMap<WatchId, WatchResponse>,
}

impl WatcherSet {
    fn new() -> Self {
        WatcherSet {
            next_watch_id: WatchId(0),
            key_to_watch_id: HashMap::new(),
            states: HashMap::new(),
            unsynced_watchers: IndexSet::new(),
            syncing_watchers: IndexSet::new(),
            fragmented_responses: HashMap::new(),
            pending_cancels: VecDeque::new(),
        }
    }

    fn is_empty(&self) -> bool {
        self.key_to_watch_id.is_empty()
    }

    /// Adds a watcher to the set.
    ///
    /// Returns a result, where Ok(InternalWatcherId) is returned if the watcher was added successfully,
    /// and Err(InternalWatcherId) is returned if a watcher with the same key already exists.
    fn add_watcher(&mut self, key: WatcherKey, value: WatcherValue) -> Result<WatchId, WatchId> {
        match self.key_to_watch_id.entry(key) {
            Entry::Occupied(ent) => Err(*ent.get()),
            Entry::Vacant(ent) => {
                let watch_id = self.next_watch_id;
                self.next_watch_id.0 += 1;

                self.states.insert(
                    watch_id,
                    WatcherState {
                        key: ent.key().clone(),
                        sync_state: WatcherSyncState::Unsynced,
                        value,
                        id: watch_id,
                        sender: broadcast::channel(1).0,
                    },
                );
                self.unsynced_watchers.insert(watch_id);
                ent.insert(watch_id);

                Ok(watch_id)
            }
        }
    }

    fn next_state_to_sync(&mut self) -> Option<&WatcherState> {
        let unsynced_id = self.unsynced_watchers.first()?;

        self.update_watcher(*unsynced_id, |state| {
            state.sync_state = WatcherSyncState::Syncing;
        })
    }

    fn id_for_key(&self, key: &WatcherKey) -> Option<WatchId> {
        self.key_to_watch_id.get(key).copied()
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
        self.key_to_watch_id.remove(&key)?;

        match sync_state {
            WatcherSyncState::Unsynced => {
                self.unsynced_watchers.shift_remove(&watch_id);
            }
            WatcherSyncState::Syncing => {
                self.syncing_watchers.shift_remove(&watch_id);

                if enqueue_pending {
                    self.pending_cancels.push_back(watch_id);
                }
            }
            WatcherSyncState::Synced => {
                if enqueue_pending {
                    self.pending_cancels.push_back(watch_id);
                }
            }
        }

        Some(key)
    }

    fn take_pending_cancel(&mut self) -> Option<WatchId> {
        self.pending_cancels.pop_front()
    }

    fn update_watcher(
        &mut self,
        watch_id: WatchId,
        f: impl FnOnce(&mut WatcherState),
    ) -> Option<&WatcherState> {
        let state = self.states.get_mut(&watch_id)?;
        let prev_sync_state = state.sync_state;
        f(state);

        use WatcherSyncState::*;
        match (&prev_sync_state, &state.sync_state) {
            (Unsynced, Synced) => {
                self.unsynced_watchers.shift_remove(&watch_id);
            }
            (Synced, Unsynced) => {
                self.unsynced_watchers.insert(watch_id);
            }
            (Syncing, Synced) => {
                self.syncing_watchers.shift_remove(&watch_id);
            }
            (Synced, Syncing) => {
                self.syncing_watchers.insert(watch_id);
            }
            (Unsynced, Syncing) => {
                self.unsynced_watchers.shift_remove(&watch_id);
                self.syncing_watchers.insert(watch_id);
            }
            (Syncing, Unsynced) => {
                self.syncing_watchers.shift_remove(&watch_id);
                self.unsynced_watchers.insert(watch_id);
            }
            (Unsynced, Unsynced) | (Syncing, Syncing) | (Synced, Synced) => {
                // no-op, no state change.
            }
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

    /// Returns the number of watchers that are currently syncing, which means we've sent a create watch request
    /// to the etcd server, and are waiting for the create response.
    fn num_syncing(&self) -> usize {
        self.syncing_watchers.len()
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
}

enum StreamingWatcherMessage {
    Disconnected(DisconnectReason),
}

impl StreamingWatcher {
    // When connecting to etcd, we'll only allow a certain number of watchers to sync concurrently, to avoid
    // overwhelming the server.
    const CONCURRENT_SYNC_LIMIT: usize = 5;

    fn new(watch_client: WatchClient<AuthedChannel>, progress_request_interval: Duration) -> Self {
        Self {
            watch_client,
            state: StreamingWatcherState::Disconnected,
            set: WatcherSet::new(),
            progress_request_interval,
        }
    }

    async fn next_message(&mut self) -> StreamingWatcherMessage {
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
                            // todo: log connected message.
                            // todo: handle response before inner?
                            let connected = ConnectedWatcherStream::new(
                                response.into_inner(),
                                self.progress_request_interval,
                                sender,
                            );
                            self.state = StreamingWatcherState::Connected(connected);
                        }
                        (Err(error), _) => {
                            // todo: log error message.
                            // todo: backoff.
                            eprintln!("Error connecting to etcd: {:?} - will retry.", error);
                            self.state = StreamingWatcherState::Reconnecting(Box::pin(
                                tokio::time::sleep(std::time::Duration::from_secs(5)),
                            ));
                        }
                    }
                }
                StreamingWatcherState::Reconnecting(sleep) => {
                    sleep.await;
                    self.do_connect();
                }
                StreamingWatcherState::Connected(connected) => {
                    let message = connected.next_message().await;
                    match message {
                        Ok(response) => {
                            self.handle_response(response);
                        }
                        Err(disconnect_reason) => {
                            // todo: reconnect timeout? backoff?
                            self.state = StreamingWatcherState::Reconnecting(Box::pin(
                                tokio::time::sleep(std::time::Duration::from_secs(5)),
                            ));
                            return StreamingWatcherMessage::Disconnected(disconnect_reason);
                        }
                    }
                }
            }
        }
    }

    fn handle_response(&mut self, response: WatchResponse) {
        let Some(response) = self.set.try_merge_fragmented_response(response) else {
            return;
        };

        let watch_id = WatchId(response.watch_id);

        if response.canceled {
            self.set.cancel_watcher(watch_id, false);
            return;
        }

        self.set.update_watcher(watch_id, |s| {
            if response.compact_revision != 0 {
                s.sync_state = WatcherSyncState::Unsynced;
                // todo: log compact revision properly, we need to re-fetch the entire key potentially?
                s.value.revision = response.compact_revision;
                return;
            }

            // the server has acknowledged the watcher, so we can mark it as synced.
            if response.created {
                s.sync_state = WatcherSyncState::Synced;
            }

            for event in response.events {
                s.update_from_event(event);
            }
        });

        self.try_sync_next();
    }

    fn add_watcher(&mut self, key: WatcherKey, value: WatcherValue) -> Result<WatchId, WatchId> {
        let watch_id = self.set.add_watcher(key, value);
        self.ensure_connected_state();

        watch_id
    }

    fn cancel_watcher(&mut self, watch_id: WatchId) -> Option<WatcherKey> {
        let key = self.set.cancel_watcher(watch_id, true);
        self.ensure_connected_state();

        // If we're connected, try and flush any pending cancel requests.
        if let StreamingWatcherState::Connected(connected) = &mut self.state {
            while let Some(watch_id) = self.set.take_pending_cancel() {
                connected.send(WatchRequest {
                    request_union: Some(watch_request::RequestUnion::CancelRequest(
                        tokio_etcd_grpc_client::WatchCancelRequest {
                            watch_id: watch_id.0,
                        },
                    )),
                });
            }
        }

        key
    }

    fn get_state_by_id(&self, id: &WatchId) -> Option<&WatcherState> {
        self.set.states.get(id)
    }

    fn get_state_by_key(&self, key: &WatcherKey) -> Option<&WatcherState> {
        self.set
            .id_for_key(key)
            .and_then(|id| self.set.states.get(&id))
    }

    fn ensure_connected_state(&mut self) {
        if self.set.is_empty() {
            // There are no watchers to sync, so we can disconnect.
            self.state = StreamingWatcherState::Disconnected;
        } else if self.state.is_disconnected() {
            // There are watchers to sync, so we should connect.
            self.do_connect();
        }
    }

    fn do_connect(&mut self) {
        // Since we're connecting, we'll mark all watchers as unsynced, since we're going to re-sync them to
        // the new connection.
        self.set.reset_for_new_connection();

        // We'll start by constructing all the initial watch requests.
        let (sender, receiver) = unbounded_channel();
        let receiver = UnboundedReceiverStream::new(receiver);
        while let Some(state) = self.set.next_state_to_sync() {
            sender.send(state.initial_watch_request()).ok();

            if self.set.num_syncing() >= Self::CONCURRENT_SYNC_LIMIT {
                break;
            }
        }

        // If we have any watchers to sync, we'll start connecting.
        if self.set.num_syncing() > 0 {
            let mut watch_client = self.watch_client.clone();
            self.state = StreamingWatcherState::Connecting(Box::pin(async move {
                let res = watch_client.watch(receiver).await;

                (res, sender)
            }));
        } else {
            // If we don't have any watchers to sync, we'll just mark ourselves as disconnected, since there
            // are no watchers to sync.
            self.state = StreamingWatcherState::Disconnected;
        }
    }

    fn try_sync_next(&mut self) {
        // If we're already syncing the maximum number of watchers, we'll just return, since we're still
        // waiting for some of them to finish syncing.
        if self.set.num_syncing() >= Self::CONCURRENT_SYNC_LIMIT {
            return;
        }

        // If we're connected, let's try and sync the next watcher.
        if let StreamingWatcherState::Connected(connected) = &mut self.state {
            if let Some(state) = self.set.next_state_to_sync() {
                connected.send(state.initial_watch_request());
            }
        }
    }
}

impl WatcherWorker {
    async fn run(mut self) {
        loop {
            enum Action {
                WorkerMessage(WorkerMessage),
                StreamDisconnected(DisconnectReason),
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
                message = self.streaming_watcher.next_message() => {
                    match message {
                        StreamingWatcherMessage::Disconnected(reason) => Action::StreamDisconnected(reason),
                    }
                }
            };

            match action {
                Action::WorkerMessage(message) => self.handle_worker_message(message),
                Action::ReadResult((key, value)) => {
                    self.handle_read_result(key, value);
                }
                Action::StreamDisconnected(reason) => {
                    // todo: log reason.
                }
            }
        }
    }

    fn handle_worker_message(&mut self, worker_message: WorkerMessage) {
        match worker_message {
            WorkerMessage::WatchKey { key, sender } => {
                self.do_watch_key(key, sender);
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

        match value {
            Ok(response) => {
                let response = response.into_inner();
                let kv = response.kvs.into_iter().next();
                let value = match kv {
                    Some(kv) => WatcherValue {
                        value: Some(kv.value.into()),
                        revision: kv.mod_revision,
                    },
                    None => WatcherValue {
                        value: None,
                        revision: response
                            .header
                            .expect("invariant: header is always present")
                            .revision,
                    },
                };
                // Now, begin watching:
                let watch_id = self
                    .streaming_watcher
                    .add_watcher(key.clone(), value)
                    .expect("invariant: the watcher should be new");

                let state = self
                    .streaming_watcher
                    .get_state_by_id(&watch_id)
                    .expect("invariant: watcher should exist");

                for sender in senders {
                    if sender.is_closed() {
                        continue;
                    }

                    sender
                        .send(Ok(InitialWatchState {
                            value: state.value.clone(),
                            receiver: state.sender.subscribe(),
                            watch_id,
                        }))
                        .ok();
                }
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

        // Check to see if we're already watching that key, so we can duplicate the watcher:
        if let Some(state) = self.streaming_watcher.get_state_by_key(&key) {
            // We indeed have the key? Let's just send the initial state to the resolver.
            let value = state.value.clone();
            sender
                .send(Ok(InitialWatchState {
                    value,
                    receiver: state.sender.subscribe(),
                    watch_id: state.id,
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

    fn new(
        rx: Receiver<WorkerMessage>,
        watch_client: WatchClient<AuthedChannel>,
        kv_client: KvClient<AuthedChannel>,
    ) -> Self {
        Self {
            rx,
            kv_client,
            in_progress_reads: Default::default(),
            streaming_watcher: StreamingWatcher::new(watch_client, Duration::from_secs(60)),
            read_join_set: JoinSet::new(),
        }
    }
}
