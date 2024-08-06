mod fsm;
mod fsm_client;

use std::{
    collections::{hash_map::Entry, HashMap, VecDeque},
    future::{pending, Future},
    panic,
    pin::Pin,
    time::Duration,
};

use fsm::{WatchCancelledByServer, WatcherValue};
use fsm_client::WatcherFsmClient;
use thiserror::Error;
use tokio::{
    sync::{
        broadcast::{self, error::RecvError},
        mpsc::{UnboundedReceiver, UnboundedSender, WeakUnboundedSender},
    },
    task::JoinSet,
};
use tokio_etcd_grpc_client::{AuthedChannel, KvClient, RangeRequest, RangeResponse, WatchClient};
use tonic::{Response, Status};

use crate::WatchId;

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
    tx: UnboundedSender<WorkerMessage>,
}

impl WatcherHandle {
    pub(crate) fn new(
        watch_client: WatchClient<AuthedChannel>,
        kv_client: KvClient<AuthedChannel>,
    ) -> Self {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

        let worker = WatcherWorker::new(rx, tx.downgrade(), watch_client, kv_client);
        tokio::spawn(worker.run());

        WatcherHandle { tx }
    }

    /// Watches a key in etcd, returning the latest value of the key, and a receiver that can be used to receive
    /// updates to the key.
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

// todo: name this better.
#[derive(Debug)]
pub struct Watched {
    /// The initial value of the watched key. This value will be the latest value of the key at the time the watch
    /// was created.
    ///
    /// Note, if the watcher was coalesced, this may not be the latest value of the key, but it will be the latest
    /// value that the watcher knows about.
    pub value: WatcherValue,

    /// This receiver can be used to receive updates to the watched key. Dropping this receiver will automatically
    /// cancel the watcher on the server.
    pub receiver: WatcherReceiver,
}

#[derive(Debug)]
enum WatchReceiverDropGuard {
    Armed {
        tx: UnboundedSender<WorkerMessage>,
        watch_id: WatchId,
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
        if let Self::Armed { tx, watch_id } = self {
            tx.send(WorkerMessage::ReceiverDropped(*watch_id)).ok();
        }
    }
}

enum ReceiverState {
    Active {
        receiver: broadcast::Receiver<Result<WatcherValue, WatchCancelledByServer>>,
        // this field must be the last field in the struct, as it must
        // be dropped after the receiver.
        drop_guard: WatchReceiverDropGuard,
    },
    Cancelled(WatchCancelledByServer),
}

pub struct WatcherReceiver {
    receiver: ReceiverState,
}

impl std::fmt::Debug for WatcherReceiver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // todo: better debug impl.
        f.debug_struct("WatcherReceiver").finish()
    }
}

impl WatcherReceiver {
    fn new(
        receiver: broadcast::Receiver<Result<WatcherValue, WatchCancelledByServer>>,
        tx: UnboundedSender<WorkerMessage>,
        watch_id: WatchId,
    ) -> Self {
        Self {
            receiver: ReceiverState::Active {
                receiver,
                drop_guard: WatchReceiverDropGuard::Armed { tx, watch_id },
            },
        }
    }

    pub async fn recv(&mut self) -> Result<WatcherValue, WatchCancelledByServer> {
        loop {
            match &mut self.receiver {
                ReceiverState::Active {
                    receiver,
                    drop_guard,
                } => {
                    match receiver.recv().await {
                        Ok(Ok(value)) => return Ok(value),
                        Ok(Err(cancelled)) => {
                            drop_guard.disarm();
                            self.receiver = ReceiverState::Cancelled(cancelled);
                        }
                        // If we have lagged, we'll skip over it and try to receive the next value.
                        Err(RecvError::Lagged(_)) => continue,
                        Err(RecvError::Closed) => {
                            self.receiver = ReceiverState::Cancelled(WatchCancelledByServer {
                                reason: "watcher receiver closed".into(),
                            });
                        }
                    }
                }
                ReceiverState::Cancelled(err) => return Err(err.clone()),
            }
        }
    }
}

#[derive(Debug, Error)]
pub enum WatchError {
    #[error("etcd error: {0}")]
    EtcdError(Status),
}

type InitialWatchSender = tokio::sync::oneshot::Sender<Result<Watched, WatchError>>;

type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;

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

struct WatcherWorker {
    rx: UnboundedReceiver<WorkerMessage>,
    weak_tx: WeakUnboundedSender<WorkerMessage>,
    in_progress_reads: HashMap<WatcherKey, Vec<InitialWatchSender>>,
    streaming_watcher: WatcherFsmClient,
    kv_client: KvClient<AuthedChannel>,
    range_request_join_set: JoinSet<(WatcherKey, Result<Response<RangeResponse>, Status>)>,
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
            streaming_watcher: WatcherFsmClient::new(watch_client, Duration::from_secs(60)),
            range_request_join_set: JoinSet::new(),
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
                        // The receiver was closed, which means that we'll no longer receive any more messages, so
                        // we can shut down.
                        tracing::info!("worker receiver closed, shutting down");
                        break;
                    }
                },
                Some(read_request_result) = self.range_request_join_set.join_next(), if !self.range_request_join_set.is_empty() => {
                    match read_request_result {
                        Ok(response) => Action::ReadResult(response),
                        Err(panic) => panic::resume_unwind(panic.into_panic()),
                    }
                }
                _ = self.streaming_watcher.next() => { continue; }
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
                self.streaming_watcher
                    .cancel_watcher_if_no_receivers(watch_id);
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
                    Some(kv) => WatcherValue::from_kv(kv),
                    None => WatcherValue::Unset { mod_revision: None },
                };

                // Now, begin watching:
                let watch_id = {
                    let state = self
                        .streaming_watcher
                        .add_watcher(key.clone(), value, revision)
                        .expect("invariant: the watcher should be new");

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
                self.streaming_watcher
                    .cancel_watcher_if_no_receivers(watch_id);
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
                self.range_request_join_set.spawn(async move {
                    let result = kv_client.range(key.make_range_request()).await;

                    (key, result)
                });
            }
        }
    }
}
