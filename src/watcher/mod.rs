mod fsm;
mod fsm_client;
mod util;

use std::{
    collections::{hash_map::Entry, HashMap},
    panic,
};

use fsm::{CancelledByServer, WatchResponse, WatcherValue};
use fsm_client::WatcherFsmClient;
use thiserror::Error;
use tokio::{
    sync::{
        broadcast::{self, error::RecvError},
        mpsc::{self, unbounded_channel, UnboundedReceiver, UnboundedSender, WeakUnboundedSender},
    },
    task::JoinSet,
};
use tokio_etcd_grpc_client::{self as pb};
use tonic::{Response, Status};
use util::range_end_for_prefix;

use crate::{ids::IdFastHasherBuilder, WatchId};

/// A high-level etcd watcher, which handles the complexity of watching keys in etcd.
///
/// Notably, this watcher will handle the following:
/// - Reconnecting to etcd and re-creating watchers on the server when the connection is lost.
/// - Coalescing watch requests, so that multiple requests to watch the same key will only result in a single watch
///   request being made to etcd.
/// - Handling fragmented watch responses, which can occur when a watch response is too large to fit in a single gRPC
///   message.
/// - Sending etcd progress requests to ensure that we're always up-to-date with the latest revisions, to ensure that
///   re-syncs are as efficient as possible.
/// - Handling watch cancellations, both from the client and the server.
pub struct WatcherHandle {
    sender: UnboundedSender<WorkerMessage>,
}

impl WatcherHandle {
    pub(crate) fn new(
        watch_client: pb::WatchClient<pb::AuthedChannel>,
        kv_client: pb::KvClient<pb::AuthedChannel>,
    ) -> Self {
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();

        let worker = WatcherWorker::new(receiver, sender.downgrade(), watch_client, kv_client);
        tokio::spawn(worker.run());

        WatcherHandle { sender }
    }

    /// Watches a key in etcd, returning the latest value of the key, and a receiver that can be used to receive
    /// updates to the key.
    ///
    /// This method will coalesce watchers for the same key into a single watcher, meaning that concurrent watches
    /// to the same key will only create a single watcher on the etcd server, and values will be broadcast
    /// to all receivers.
    ///
    /// Note: The most recent copy of the key is stored in memory in order to coalesce the watcher. If this behavior
    /// is not desired, use [`WatcherHandle::watch_with_config`] directly.
    pub async fn watch_key_coalesced(
        &self,
        key: impl Into<Key>,
    ) -> Result<CoalescedWatch, WatchError> {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        self.sender
            .send(WorkerMessage::CoalescedWatchKey {
                key: key.into(),
                sender,
            })
            .ok();

        receiver
            .await
            .expect("invariant: worker has unexpectedly shutdown")
    }

    /// Watches a key in etcd, using the given [`WatchConfig`].
    ///
    /// The watcher will be automatically cancelled when the returned [`ForwardedWatchReceiver`] is dropped.
    pub async fn watch_with_config(&self, watch_config: WatchConfig) -> ForwardedWatchReceiver {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        self.sender
            .send(WorkerMessage::WatchAndForward {
                watch_config,
                sender,
            })
            .ok();

        receiver
            .await
            .expect("invariant: worker has unexpectedly shutdown")
    }
}

enum WorkerMessage {
    CoalescedWatchKey {
        key: Key,
        sender: InitialCoalescedWatchSender,
    },
    WatchAndForward {
        watch_config: WatchConfig,
        sender: InitialForwardedWatchSender,
    },
    ReceiverDropped(WatchId),
}

/// Returned by [`WatcherHandle::watch_key_coalesced`]
#[derive(Debug)]
pub struct CoalescedWatch {
    /// The initial value of the watched key. This value will be the latest value of the key at the time the watch
    /// was created.
    ///
    /// Note, if the watcher was coalesced, this may not be the latest value of the key, but it will be the latest
    /// value that the watcher knows about.
    pub value: WatcherValue,

    /// This receiver can be used to receive updates to the watched key. Dropping this receiver will automatically
    /// cancel the watcher on the server.
    pub receiver: CoalescedWatcherReceiver,
}

#[derive(Debug)]
enum WatchReceiverDropGuard {
    Armed {
        worker_sender: UnboundedSender<WorkerMessage>,
        id: WatchId,
    },
    Disarmed,
}

impl WatchReceiverDropGuard {
    fn disarm(&mut self) {
        *self = WatchReceiverDropGuard::Disarmed;
    }
}

impl Drop for WatchReceiverDropGuard {
    fn drop(&mut self) {
        if let Self::Armed { worker_sender, id } = self {
            worker_sender.send(WorkerMessage::ReceiverDropped(*id)).ok();
        }
    }
}

enum ReceiverState<ReceiverT> {
    Active {
        receiver: ReceiverT,
        // this field must be the last field in the struct, as it must
        // be dropped after the receiver.
        drop_guard: WatchReceiverDropGuard,
    },
    CancelledByServer(CancelledByServer),
}

pub struct CoalescedWatcherReceiver {
    state: ReceiverState<broadcast::Receiver<Result<WatcherValue, CancelledByServer>>>,
}

impl std::fmt::Debug for CoalescedWatcherReceiver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // todo: better debug impl.
        f.debug_struct("WatcherReceiver").finish()
    }
}

impl CoalescedWatcherReceiver {
    fn new(
        receiver: broadcast::Receiver<Result<WatcherValue, CancelledByServer>>,
        worker_sender: UnboundedSender<WorkerMessage>,
        id: WatchId,
    ) -> Self {
        Self {
            state: ReceiverState::Active {
                receiver,
                drop_guard: WatchReceiverDropGuard::Armed { worker_sender, id },
            },
        }
    }

    pub async fn recv(&mut self) -> Result<WatcherValue, CancelledByServer> {
        loop {
            match &mut self.state {
                ReceiverState::Active {
                    receiver,
                    drop_guard,
                } => {
                    match receiver.recv().await {
                        Ok(Ok(value)) => return Ok(value),
                        Ok(Err(cancelled)) => {
                            drop_guard.disarm();
                            self.state = ReceiverState::CancelledByServer(cancelled);
                        }
                        // If we have lagged, we'll skip over it and try to receive the next value.
                        Err(RecvError::Lagged(_)) => continue,
                        Err(RecvError::Closed) => {
                            self.state = ReceiverState::CancelledByServer(CancelledByServer {
                                reason: "watcher receiver closed".into(),
                            });
                        }
                    }
                }
                ReceiverState::CancelledByServer(err) => return Err(err.clone()),
            }
        }
    }
}

/// Returned by [`WatcherHandle::watch_with_config`]
pub struct ForwardedWatchReceiver {
    state: ReceiverState<UnboundedReceiver<WatchResponse>>,
}

impl ForwardedWatchReceiver {
    fn new(
        receiver: UnboundedReceiver<WatchResponse>,
        worker_sender: UnboundedSender<WorkerMessage>,
        id: WatchId,
    ) -> Self {
        Self {
            state: ReceiverState::Active {
                receiver,
                drop_guard: WatchReceiverDropGuard::Armed { worker_sender, id },
            },
        }
    }

    /// Receives a [`WatchResponse`] from the server.
    ///
    /// Once [`WatchResponse::Cancelled`] is returned, the watcher will return no more events, and the watcher
    /// must be re-established.
    pub async fn recv(&mut self) -> WatchResponse {
        match &mut self.state {
            ReceiverState::Active {
                receiver,
                drop_guard,
            } => match receiver.recv().await {
                Some(WatchResponse::CancelledByServer(reason)) => {
                    drop_guard.disarm();
                    self.state = ReceiverState::CancelledByServer(reason.clone());
                    WatchResponse::CancelledByServer(reason)
                }
                Some(response) => response,
                None => {
                    let reason = CancelledByServer {
                        reason: "watcher receiver closed".into(),
                    };
                    self.state = ReceiverState::CancelledByServer(reason.clone());
                    WatchResponse::CancelledByServer(reason)
                }
            },
            ReceiverState::CancelledByServer(reason) => {
                WatchResponse::CancelledByServer(reason.clone())
            }
        }
    }
}

#[derive(Debug, Error)]
pub enum WatchError {
    #[error("etcd error: {0}")]
    EtcdError(Status),
}

type InitialCoalescedWatchSender = tokio::sync::oneshot::Sender<Result<CoalescedWatch, WatchError>>;
type InitialForwardedWatchSender = tokio::sync::oneshot::Sender<ForwardedWatchReceiver>;

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct Key(Box<[u8]>);

impl From<String> for Key {
    fn from(value: String) -> Self {
        Self(value.into_bytes().into())
    }
}

impl From<Vec<u8>> for Key {
    fn from(value: Vec<u8>) -> Self {
        Self(value.into())
    }
}

impl From<Box<[u8]>> for Key {
    fn from(value: Box<[u8]>) -> Self {
        Self(value)
    }
}

impl From<&str> for Key {
    fn from(value: &str) -> Self {
        Self(value.as_bytes().to_owned().into())
    }
}

impl From<&[u8]> for Key {
    fn from(value: &[u8]) -> Self {
        Self(value.into())
    }
}

impl Key {
    // fixme: this really should not be on `Key`
    fn make_range_request(&self) -> pb::RangeRequest {
        pb::RangeRequest {
            key: self.as_vec(),
            ..Default::default()
        }
    }

    fn as_vec(&self) -> Vec<u8> {
        self.0.clone().into_vec()
    }

    fn as_slice(&self) -> &[u8] {
        &self.0
    }
}

struct KeyWatchState {
    id: WatchId,
    watch_kind: WatchKind,
}
impl KeyWatchState {
    fn send_response(&mut self, response: WatchResponse) -> bool {
        let is_cancelled = response.is_cancelled();

        match &mut self.watch_kind {
            WatchKind::CoalescedKey {
                value,
                broadcast_sender,
                ..
            } => {
                if let WatchResponse::Events { events, .. } = response {
                    for event in events {
                        *value = event.value;
                        broadcast_sender.send(Ok(value.clone())).ok();
                    }
                }
            }
            WatchKind::ForwardWatchResponses { sender } => {
                sender.send(response).ok();
            }
        }

        !is_cancelled
    }

    fn has_no_receiver(&self) -> bool {
        match &self.watch_kind {
            WatchKind::CoalescedKey {
                broadcast_sender, ..
            } => broadcast_sender.receiver_count() == 0,
            WatchKind::ForwardWatchResponses { sender } => sender.is_closed(),
        }
    }
}

enum WatchKind {
    CoalescedKey {
        key: Key,
        value: WatcherValue,
        broadcast_sender: broadcast::Sender<Result<WatcherValue, CancelledByServer>>,
    },
    ForwardWatchResponses {
        sender: mpsc::UnboundedSender<WatchResponse>,
    },
}

impl WatchKind {}

#[derive(Default)]
struct WatchedSet {
    states: HashMap<WatchId, KeyWatchState, IdFastHasherBuilder>,
    coalesced_keys: HashMap<Key, WatchId>,
}

#[derive(Error, Debug)]
enum InsertError {
    #[error("key already exists (existing watch id: {existing_watch_id:?}")]
    KeyAlreadyExists { existing_watch_id: WatchId },
    #[error("watch id already exists")]
    WatchIdAlreadyExists,
}

impl WatchedSet {
    fn get_watch_state_by_key(&self, key: &Key) -> Option<&KeyWatchState> {
        let watch_id = self.coalesced_keys.get(key)?;
        Some(&self.states[watch_id])
    }

    fn update_from_watch_response(&mut self, id: WatchId, response: WatchResponse) {
        if let Some(state) = self.states.get_mut(&id) {
            if !state.send_response(response) {
                self.remove(id);
            }
        }
    }

    fn insert(
        &mut self,
        id: WatchId,
        watch_kind: WatchKind,
    ) -> Result<&KeyWatchState, InsertError> {
        match watch_kind {
            WatchKind::CoalescedKey {
                key,
                value,
                broadcast_sender,
            } => {
                let vacant_key = match self.coalesced_keys.entry(key) {
                    Entry::Occupied(ent) => {
                        return Err(InsertError::KeyAlreadyExists {
                            existing_watch_id: *ent.get(),
                        })
                    }
                    Entry::Vacant(ent) => ent,
                };

                let vacant_watch_id = match self.states.entry(id) {
                    Entry::Occupied(_) => return Err(InsertError::WatchIdAlreadyExists),
                    Entry::Vacant(ent) => ent,
                };

                let key = vacant_key.key().clone();
                vacant_key.insert(id);
                Ok(vacant_watch_id.insert(KeyWatchState {
                    id,
                    watch_kind: WatchKind::CoalescedKey {
                        key,
                        value,
                        broadcast_sender,
                    },
                }))
            }
            WatchKind::ForwardWatchResponses { sender } => {
                let vacant_watch_id = match self.states.entry(id) {
                    Entry::Occupied(_) => return Err(InsertError::WatchIdAlreadyExists),
                    Entry::Vacant(ent) => ent,
                };

                Ok(vacant_watch_id.insert(KeyWatchState {
                    id,
                    watch_kind: WatchKind::ForwardWatchResponses { sender },
                }))
            }
        }
    }

    fn remove(&mut self, id: WatchId) -> Option<KeyWatchState> {
        let state = self.states.remove(&id)?;
        if let WatchKind::CoalescedKey { key, .. } = &state.watch_kind {
            let removed_id = self
                .coalesced_keys
                .remove(&key)
                .expect("invariant: key must exist in key_to_watch_id");
            assert_eq!(id, removed_id, "invariant: watch id should be the same");
        }

        Some(state)
    }

    fn cancel_watcher_if_no_receiver(&mut self, id: WatchId) -> bool {
        let has_no_receiver = match self.states.get(&id) {
            Some(state) => state.has_no_receiver(),
            None => false,
        };

        if has_no_receiver {
            self.remove(id);
        }

        has_no_receiver
    }
}

struct WatcherWorker {
    receiver: UnboundedReceiver<WorkerMessage>,
    weak_sender: WeakUnboundedSender<WorkerMessage>,
    in_progress_reads: HashMap<Key, Vec<InitialCoalescedWatchSender>>,
    watcher_fsm_client: WatcherFsmClient,
    watched: WatchedSet,
    kv_client: pb::KvClient<pb::AuthedChannel>,
    range_request_join_set: JoinSet<(Key, Result<Response<pb::RangeResponse>, Status>)>,
}

impl WatcherWorker {
    const CONCURRENT_SYNC_LIMIT: usize = 5;
    const BROADCAST_CHANNEL_CAPACITY: usize = 16;

    fn new(
        receiver: UnboundedReceiver<WorkerMessage>,
        weak_sender: WeakUnboundedSender<WorkerMessage>,
        watch_client: pb::WatchClient<pb::AuthedChannel>,
        kv_client: pb::KvClient<pb::AuthedChannel>,
    ) -> Self {
        Self {
            receiver,
            weak_sender,
            kv_client,
            in_progress_reads: Default::default(),
            watched: Default::default(),
            watcher_fsm_client: WatcherFsmClient::new(watch_client, Self::CONCURRENT_SYNC_LIMIT),
            range_request_join_set: JoinSet::new(),
        }
    }

    async fn run(mut self) {
        loop {
            enum Action {
                WorkerMessage(WorkerMessage),
                ReadResult(Key, Result<Response<pb::RangeResponse>, Status>),
                WatchResponse(WatchId, WatchResponse),
            }

            let action = tokio::select! {
                message = self.receiver.recv() => {
                    if let Some(message) = message {
                        Action::WorkerMessage(message)
                    } else {
                        // The receiver was closed, which means that we'll no longer receive any more messages, so
                        // we can shut down.
                        tracing::info!("worker receiver closed, shutting down");
                        break;
                    }
                },
                Some(read_request_result) = self.range_request_join_set.join_next(), if !self.range_request_join_set.is_empty() => {
                    match read_request_result {
                        Ok((key, result)) => Action::ReadResult(key, result),
                        Err(panic) => panic::resume_unwind(panic.into_panic()),
                    }
                }
                (watch_id, response) = self.watcher_fsm_client.next() => Action::WatchResponse(watch_id, response),
            };

            match action {
                Action::WorkerMessage(message) => self.handle_worker_message(message),
                Action::WatchResponse(watch_id, response) => {
                    self.handle_watch_response(watch_id, response);
                }
                Action::ReadResult(key, value) => {
                    self.handle_read_result(key, value);
                }
            }
        }
    }

    fn handle_worker_message(&mut self, worker_message: WorkerMessage) {
        match worker_message {
            WorkerMessage::CoalescedWatchKey { key, sender } => {
                self.do_coalesced_watch(key, sender);
            }
            WorkerMessage::WatchAndForward {
                watch_config,
                sender,
            } => {
                self.do_watch_and_forward(watch_config, sender);
            }
            WorkerMessage::ReceiverDropped(watch_id) => {
                self.cancel_watcher_if_no_receiver(watch_id);
            }
        }
    }

    fn cancel_watcher_if_no_receiver(&mut self, watch_id: WatchId) {
        if self.watched.cancel_watcher_if_no_receiver(watch_id) {
            self.watcher_fsm_client.cancel_watcher(watch_id);
        }
    }

    fn handle_read_result(&mut self, key: Key, value: Result<Response<pb::RangeResponse>, Status>) {
        let Some(senders) = self.in_progress_reads.remove(&key) else {
            tracing::warn!(
                "received read result for key that isn't in progress: {key:?}. ignoring result"
            );
            return;
        };

        let Some(worker_sender) = self.weak_sender.upgrade() else {
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
                    Some(kv) => WatcherValue::from_kv(kv),
                    None => WatcherValue::Unset { mod_revision: None },
                };

                // Now, begin watching:
                let id = {
                    let id = self.watcher_fsm_client.add_watcher(
                        WatchConfig::for_single_key(key.clone()).with_start_revision(revision + 1),
                    );
                    let (broadcast_sender, receiver) =
                        broadcast::channel(Self::BROADCAST_CHANNEL_CAPACITY);

                    let state = self
                        .watched
                        .insert(
                            id,
                            WatchKind::CoalescedKey {
                                key,
                                value: value.clone(),
                                broadcast_sender,
                            },
                        )
                        .expect("invariant: insert should not fail");

                    for sender in senders {
                        if sender.is_closed() {
                            continue;
                        }

                        sender
                            .send(Ok(CoalescedWatch {
                                value: value.clone(),
                                receiver: CoalescedWatcherReceiver::new(
                                    receiver.resubscribe(),
                                    worker_sender.clone(),
                                    state.id,
                                ),
                            }))
                            .ok();
                    }

                    id
                };

                // If somehow all senders were dropped, we can detect that here and cancel the watcher.
                self.cancel_watcher_if_no_receiver(id);
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

    fn handle_watch_response(&mut self, id: WatchId, response: WatchResponse) {
        self.watched.update_from_watch_response(id, response);
    }

    fn do_coalesced_watch(&mut self, key: Key, initial_sender: InitialCoalescedWatchSender) {
        // If the sender is closed, we'll just return, since we can't send the result.
        if initial_sender.is_closed() {
            return;
        }

        // We should be able to upgrade if there is a handle to the worker which sent us this request. If not,
        // there's no worker handle, so we'll just return.
        let Some(sender) = self.weak_sender.upgrade() else {
            return;
        };

        // Check to see if we're already watching that key, so we can duplicate the watcher:
        if let Some(state) = self.watched.get_watch_state_by_key(&key) {
            let (value, receiver) = match &state.watch_kind {
                WatchKind::CoalescedKey {
                    value,
                    broadcast_sender,
                    ..
                } => (value.clone(), broadcast_sender.subscribe()),
                WatchKind::ForwardWatchResponses { .. } => {
                    unreachable!("unreachable: we should only have coalesced key watchers")
                }
            };

            // We indeed have the key? Let's just send the initial state to the resolver.
            initial_sender
                .send(Ok(CoalescedWatch {
                    value,
                    receiver: CoalescedWatcherReceiver::new(receiver, sender, state.id),
                }))
                .ok();

            return;
        }

        // Otherwise, we'll need to try and fetch the key from etcd, and then add a watcher.
        match self.in_progress_reads.entry(key) {
            Entry::Occupied(mut ent) => ent.get_mut().push(initial_sender),
            Entry::Vacant(ent) => {
                let key = ent.key().clone();
                ent.insert(vec![initial_sender]);

                let mut kv_client = self.kv_client.clone();
                self.range_request_join_set.spawn(async move {
                    let result = kv_client.range(key.make_range_request()).await;

                    (key, result)
                });
            }
        }
    }

    fn do_watch_and_forward(
        &mut self,
        watch_config: WatchConfig,
        initial_sender: InitialForwardedWatchSender,
    ) {
        // We should be able to upgrade if there is a handle to the worker which sent us this request. If not,
        // there's no worker handle, so we'll just return.
        let Some(worker_sender) = self.weak_sender.upgrade() else {
            return;
        };

        let id = self.watcher_fsm_client.add_watcher(watch_config);
        let (sender, receiver) = unbounded_channel();
        self.watched
            .insert(id, WatchKind::ForwardWatchResponses { sender })
            .expect("invariant: insert should always succeed");

        initial_sender
            .send(ForwardedWatchReceiver::new(receiver, worker_sender, id))
            .ok();
    }
}

/// Configuration that is used with [`WatcherHandle::watch_with_config`].
pub struct WatchConfig {
    key: Key,
    range_end: Option<Key>,
    events: WatchEvents,
    prev_kv: bool,
    start_revision: Option<i64>,
}

impl WatchConfig {
    fn with_key_and_range_end(key: Key, range_end: Option<Key>) -> Self {
        Self {
            key,
            range_end,
            events: WatchEvents::all(),
            prev_kv: false,
            start_revision: None,
        }
    }

    /// Watches a singular key.
    ///
    /// Generally, you should prefer to use [`WatcherHandle::watch_key_coalesced`], unless you need to configure
    /// the watcher additional option (see the `with_*` methods on [`WatchConfig`]).
    pub fn for_single_key(key: Key) -> Self {
        Self::with_key_and_range_end(key, None)
    }

    /// Watches all keys with the given prefix.
    ///
    /// Note: The key must be non-empty or an error is returned. If you want to watch all keys, use the
    /// [`Self::for_all_keys`] method instead.
    pub fn for_keys_with_prefix(prefix: Key) -> Self {
        let range_end = Key::from(range_end_for_prefix(prefix.as_slice()));
        Self::with_key_and_range_end(prefix, Some(range_end))
    }

    /// Watches all keys on the server.
    ///
    /// Note: Depending on the data in your etcd server, this can be a very busy watcher, so use with caution.
    pub fn for_all_keys() -> Self {
        let null_key = Key::from(&[0][..]);
        Self::with_key_and_range_end(null_key.clone(), Some(null_key))
    }

    /// Watches keys that are greater than or equal to the provided `key`.
    pub fn for_keys_greater_than_or_equal_to(key: Key) -> Self {
        Self::with_key_and_range_end(key, Some(Key::from(&[0][..])))
    }

    /// Starts the watcher with the given revision.
    ///
    /// Default: starts at the latest revision at the time the watcher is received by etcd.
    pub fn with_start_revision(mut self, revision: i64) -> Self {
        self.start_revision = Some(revision);
        self
    }

    /// Allows you to specify a subset of events to be received by the watcher.
    ///
    /// Default: all events are received.
    pub fn with_events(mut self, events: WatchEvents) -> Self {
        self.events = events;
        self
    }

    /// Returns the previous value when a value is updated or deleted.
    ///
    /// Default: false
    pub fn with_prev_kv(mut self, prev_kv: bool) -> Self {
        self.prev_kv = prev_kv;
        self
    }
}

pub struct WatchEvents {
    put: bool,
    delete: bool,
}

impl WatchEvents {
    /// Watch should include all events.
    pub fn all() -> Self {
        Self {
            put: true,
            delete: true,
        }
    }

    /// Only watch for puts
    pub fn only_put() -> Self {
        Self {
            put: true,
            delete: false,
        }
    }

    /// Only watch for deletes
    pub fn only_delete() -> Self {
        Self {
            put: false,
            delete: true,
        }
    }

    /// Convert this to a list of filter types for proto.
    fn for_filters_proto(&self) -> Vec<i32> {
        let mut data =
            Vec::with_capacity(if self.put { 0 } else { 1 } + if self.delete { 0 } else { 1 });

        if !self.put {
            data.push(pb::watch_create_request::FilterType::Noput as i32);
        }

        if !self.delete {
            data.push(pb::watch_create_request::FilterType::Nodelete as i32);
        }

        data
    }
}
