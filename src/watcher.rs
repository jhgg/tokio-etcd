use std::{
    collections::{hash_map::Entry, HashMap},
    future::{pending, Future},
    panic,
    pin::Pin,
    time::Duration,
};

use indexmap::IndexSet;
use tokio::{
    sync::mpsc::{unbounded_channel, Receiver, Sender, UnboundedSender},
    task::JoinSet,
    time::{Interval, Sleep},
};
use tokio_etcd_grpc_client::{
    watch_request, AuthedChannel, EventType, KvClient, RangeRequest, RangeResponse, WatchClient,
    WatchProgressRequest, WatchRequest, WatchResponse,
};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::{Response, Status, Streaming};

pub struct Watcher {
    tx: Sender<WorkerMessage>,
}

impl Watcher {
    pub fn new(watch_client: WatchClient<AuthedChannel>) -> Self {
        let (tx, rx) = tokio::sync::mpsc::channel(1);

        // let worker = WatcherWorker { watch_client, rx };
        // tokio::spawn(worker.run());

        Watcher { tx }
    }
}

enum WorkerMessage {
    WatchKey {
        key: WatcherKey,
        resolver: tokio::sync::oneshot::Sender<InitialWatchState>,
    },
}

struct WatchedKey {
    sender: tokio::sync::broadcast::Sender<WatchedValue>,
    revision: i64,
}

#[derive(Clone)]
struct WatchedValue {}

enum WatchState {
    Pending {
        sender: tokio::sync::broadcast::Sender<WatchedValue>,
        resolvers: Vec<tokio::sync::oneshot::Sender<InitialWatchState>>,
    },
    Watched(WatchedKey),
}

struct InitialWatchState {
    value: Option<Vec<u8>>,
    revision: i64,
}

struct WatcherWorker {
    rx: Receiver<WorkerMessage>,
    // todo: tinyvec?
    in_progress_reads: HashMap<WatcherKey, Vec<tokio::sync::oneshot::Sender<InitialWatchState>>>,
    streaming_watcher: StreamingWatcher,
    kv_client: KvClient<AuthedChannel>,
    read_join_set: JoinSet<(WatcherKey, Result<Response<RangeResponse>, Status>)>,
}

type BoxFuture<T> = Pin<Box<dyn Future<Output = T>>>;

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
struct WatchId(i64);

#[derive(Clone, PartialEq, Eq, Hash)]
enum WatcherKey {
    Key(Vec<u8>),
    // fixme: we should have a better way to represent a watcher.
    // Prefix(String),
}
impl WatcherKey {
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
    key: WatcherKey,
    sync_state: WatcherSyncState,
    watch_id: WatchId,
    value: Option<Vec<u8>>,
    revision: i64,
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
                    watch_id: self.watch_id.0,
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
    // fixme: maybe make VecDeque?
    unsynced_watchers: IndexSet<WatchId>,
    syncing_watchers: IndexSet<WatchId>,
}

impl WatcherSet {
    fn new() -> Self {
        WatcherSet {
            next_watch_id: WatchId(0),
            key_to_watch_id: HashMap::new(),
            states: HashMap::new(),
            unsynced_watchers: IndexSet::new(),
            syncing_watchers: IndexSet::new(),
        }
    }

    fn is_empty(&self) -> bool {
        self.key_to_watch_id.is_empty()
    }

    /// Adds a watcher to the set.
    ///
    /// Returns a result, where Ok(InternalWatcherId) is returned if the watcher was added successfully,
    /// and Err(InternalWatcherId) is returned if a watcher with the same key already exists.
    fn add_watcher(
        &mut self,
        key: WatcherKey,
        last_value: Option<Vec<u8>>,
        last_revision: i64,
    ) -> Result<WatchId, WatchId> {
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
                        value: last_value,
                        revision: last_revision,
                        watch_id,
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

    fn remove_watcher(&mut self, watch_id: WatchId) -> Option<WatcherKey> {
        let WatcherState { key, .. } = self.states.remove(&watch_id)?;
        self.key_to_watch_id.remove(&key)?;
        self.unsynced_watchers.shift_remove(&watch_id);
        Some(key)
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

    fn mark_all_unsynced(&mut self) {
        for state in self.states.values_mut() {
            state.sync_state = WatcherSyncState::Unsynced;
        }
        self.syncing_watchers.clear();
        self.unsynced_watchers.clear();
        self.unsynced_watchers.extend(self.states.keys());
    }

    fn num_syncing(&self) -> usize {
        self.syncing_watchers.len()
    }
}

enum StreamingWatcherMessage {
    WatchResponse(()),
    Disconnected(DisconnectReason),
}

impl StreamingWatcher {
    const CONCURRENT_SYNC_LIMIT: usize = 5;

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
                            return StreamingWatcherMessage::WatchResponse(());
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

    fn handle_response(&mut self, response: WatchResponse) -> WatchResponse {
        self.set.update_watcher(WatchId(response.watch_id), |s| {
            if response.compact_revision != 0 {
                s.sync_state = WatcherSyncState::Unsynced;
                // todo: log compact revision properly, we need to re-fetch the entire key potentially?
                s.revision = response.compact_revision;

                return;
            }

            // the server has acknowledged the watcher, so we can mark it as synced.
            if response.created {
                s.sync_state = WatcherSyncState::Synced;
            }

            // take the last event and update the state.
            if let Some(evt) = response.events.last() {
                let kv = evt.kv.as_ref().expect("invariant: kv is always present");

                s.revision = kv.mod_revision;
                match evt.r#type() {
                    EventType::Put => {
                        s.value = Some(kv.value.clone());
                    }
                    EventType::Delete => {
                        s.value = None;
                    }
                }
            }
        });

        self.try_sync_next();

        response
    }

    fn add_watcher(
        &mut self,
        key: WatcherKey,
        last_value: Option<Vec<u8>>,
        last_revision: i64,
    ) -> Result<WatchId, WatchId> {
        // fixme: there has to be a better way to write this.
        let watch_id = self.set.add_watcher(key, last_value, last_revision);
        self.ensure_connected_state();

        watch_id
    }

    fn get_watcher_by_key(&self, key: &WatcherKey) -> Option<&WatcherState> {
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
        self.set.mark_all_unsynced();

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
                WatchResponse(WatchResponse),
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
                        StreamingWatcherMessage::WatchResponse(()) => Action::WatchResponse(todo!()),
                        StreamingWatcherMessage::Disconnected(reason) => Action::StreamDisconnected(reason),
                    }
                }
            };

            match action {
                Action::WorkerMessage(message) => match message {
                    WorkerMessage::WatchKey { key, resolver } => {
                        self.do_watch_key(key, resolver);
                    }
                },
                Action::WatchResponse(response) => {
                    // todo:
                }
                Action::ReadResult((key, value)) => {}
                Action::StreamDisconnected(reason) => {
                    // todo: log reason.
                }
            }
        }
    }

    fn do_watch_key(
        &mut self,
        key: WatcherKey,
        resolve: tokio::sync::oneshot::Sender<InitialWatchState>,
    ) {
        // First, check to see if we're already watching that key, so we can duplicate the watcher:
        if let Some(state) = self.streaming_watcher.get_watcher_by_key(&key) {
            // We indeed have the key? Let's just send the initial state to the resolver.
            let value = state.value.clone();
            resolve
                .send(InitialWatchState {
                    value,
                    revision: state.revision,
                })
                .ok();

            return;
        }

        // Otherwise, we'll need to try and fetch the key from etcd, and then add a watcher.
        match self.in_progress_reads.entry(key) {
            Entry::Occupied(mut ent) => ent.get_mut().push(resolve),
            Entry::Vacant(ent) => {
                let key = ent.key().clone();
                ent.insert(vec![resolve]);

                let mut kv_client = self.kv_client.clone();
                self.read_join_set.spawn(async move {
                    let result = kv_client.range(key.make_range_request()).await;

                    (key, result)
                });
            }
        }

        // match self.watcher_map.entry(key) {
        //     Entry::Occupied(mut ent) => match ent.get_mut() {
        //         WatchState::Pending { resolvers, .. } => {
        //             resolvers.push(resolve);
        //         }
        //         WatchState::Watched(wk) => {
        //             let receiver = wk.sender.subscribe();
        //             let _ = resolve.send(InitialWatchState { receiver });
        //         }
        //     },
        //     Entry::Vacant(ent) => {
        //         let (sender, rx) = tokio::sync::broadcast::channel(16);
        //         // self.join_set.spawn(async move {
        //         //     let mut stream = self.watch_client.watch(key).await.unwrap();
        //         //     // while let Some(resp) = stream.message().await.unwrap() {
        //         //     let _ = sender.send(WatchedValue {});
        //         // }
        //         // });

        //         ent.insert(WatchState::Pending {
        //             sender,
        //             resolvers: vec![resolve],
        //         });
        //     }
        // }

        // todo!()
    }
}
