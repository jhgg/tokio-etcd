use std::{future::pending, pin::Pin, time::Duration};

use tokio::{
    sync::mpsc::{unbounded_channel, UnboundedSender},
    time::{Interval, Sleep},
};
use tokio_etcd_grpc_client::{
    watch_request, AuthedChannel, WatchClient, WatchProgressRequest, WatchRequest, WatchResponse,
    PROGRESS_WATCH_ID,
};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::{Response, Status, Streaming};

use crate::{utils::backoff::ExponentialBackoff, WatchId};

use super::{
    fsm::{ProcessedWatchResponse, WatcherFsm},
    BoxFuture, Key,
};

pub(crate) struct WatcherFsmClient {
    watch_client: WatchClient<AuthedChannel>,
    connection_state: ConnectionState,
    watcher_fsm: WatcherFsm,
    progress_request_interval: Duration,
    connection_incarnation: u64,
    backoff: ExponentialBackoff,
}

impl WatcherFsmClient {
    pub fn new(
        watch_client: WatchClient<AuthedChannel>,
        progress_request_interval: Duration,
        concurrent_sync_limit: usize,
    ) -> Self {
        Self {
            watch_client,
            watcher_fsm: WatcherFsm::new(concurrent_sync_limit),
            connection_state: ConnectionState::Disconnected,
            connection_incarnation: 0,
            progress_request_interval,
            backoff: ExponentialBackoff::new(Duration::from_secs(1), Duration::from_secs(10)),
        }
    }

    pub fn add_watcher(&mut self, key: Key, revision: i64) -> WatchId {
        // fixme: can we get rid of the clone?
        let watch_id = self.do_fsm_action(|fsm| fsm.add_watcher(key.clone(), revision));
        tracing::info!("added watcher, key: {:?}, watch_id: {:?}", key, watch_id);

        watch_id
    }

    pub fn cancel_watcher(&mut self, watch_id: WatchId) -> Option<Key> {
        let key = self.do_fsm_action(|fsm| fsm.cancel_watcher(watch_id))?;
        tracing::info!(
            "cancelled watcher, key: {:?}, watch_id: {:?}",
            key,
            watch_id
        );

        Some(key)
    }

    /// This future is cancel safe.
    ///
    /// Progresses the connection forward.
    pub async fn next(&mut self) -> (WatchId, ProcessedWatchResponse) {
        loop {
            match &mut self.connection_state {
                // When we're disconnected, we'll just stay pending forever, as we'll expect this
                // future to be cancelled by the caller.
                ConnectionState::Disconnected => {
                    return pending().await;
                }
                // We're connecting to the etcd server.
                ConnectionState::Connecting(connecting) => match connecting.await {
                    (Ok(response), sender) => {
                        let connected = ConnectedWatcherStream::new(
                            response.into_inner(),
                            self.progress_request_interval,
                            sender,
                        );
                        self.connection_state = ConnectionState::Connected(connected);
                        self.backoff.succeed();
                        tracing::info!(
                            "connected to etcd successfully, incarnation: {}",
                            self.connection_incarnation
                        );
                    }
                    (Err(error), _) => {
                        let reconnect_delay = self.backoff.fail();
                        self.connection_state = ConnectionState::Reconnecting(Box::pin(
                            tokio::time::sleep(reconnect_delay),
                        ));
                        tracing::error!(
                                "failed to connect to etcd: {:?}, incarnation: {}. will reconnect in {:?}.",
                                error,
                                self.connection_incarnation,
                                reconnect_delay
                            );
                    }
                },
                // We're reconnecting after being disconnected, we need to sleep before we can reconnect.
                ConnectionState::Reconnecting(sleep) => {
                    sleep.await;
                    self.do_connect();
                }
                // We're connected, receive a message and handle it.
                ConnectionState::Connected(connected) => match connected.next_message().await {
                    Ok(response) => {
                        if let Some(response) =
                            self.do_fsm_action(|fsm| fsm.process_watch_response(response))
                        {
                            return response;
                        }
                    }
                    Err(disconnect_reason) => {
                        let reconnect_delay = self.backoff.fail();
                        self.connection_state = ConnectionState::Reconnecting(Box::pin(
                            tokio::time::sleep(reconnect_delay),
                        ));
                        tracing::warn!(
                                "disconnected from etcd reason: {:?}, incarnation: {}. will reconnect in {:?}",
                                disconnect_reason,
                                self.connection_incarnation,
                                reconnect_delay
                            );
                    }
                },
            }
        }
    }

    /// Convenience method:
    ///
    /// Do something with the FSM, and then synchronize the connection.
    fn do_fsm_action<T>(&mut self, func: impl FnOnce(&mut WatcherFsm) -> T) -> T {
        let result = func(&mut self.watcher_fsm);
        self.sync_connection();
        result
    }

    fn sync_connection(&mut self) {
        if self.watcher_fsm.is_empty() {
            // There are no watchers to sync, so we can disconnect.
            if self.connection_state.is_disconnected() {
                return;
            }
            tracing::info!("no watchers to sync, disconnecting");
            self.connection_state = ConnectionState::Disconnected;
        } else if self.connection_state.is_disconnected() {
            // There are watchers to sync, so we should connect.
            self.do_connect();
        } else if let Some(connected) = self.connection_state.connected() {
            while let Some(watch_request) = self.watcher_fsm.next_watch_request_to_send() {
                connected.send(watch_request);
            }
        }
    }

    fn do_connect(&mut self) {
        self.watcher_fsm.reset_for_new_connection();

        if self.watcher_fsm.is_empty() {
            self.connection_state = ConnectionState::Disconnected;
            return;
        }

        self.connection_incarnation += 1;

        tracing::info!(
            "connecting to etcd for new watcher set (incarnation: {}, unsynced: {})",
            self.connection_incarnation,
            self.watcher_fsm.len(),
        );

        // We'll start by constructing all the initial watch requests.
        let (sender, receiver) = unbounded_channel();
        while let Some(watch_request) = self.watcher_fsm.next_watch_request_to_send() {
            sender.send(watch_request).ok();
        }

        // Then we connect, and send the requests.
        let mut watch_client = self.watch_client.clone();
        self.connection_state = ConnectionState::Connecting(Box::pin(async move {
            let res = watch_client
                .watch(UnboundedReceiverStream::new(receiver))
                .await;

            (res, sender)
        }));
    }
}

enum ConnectionState {
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

impl ConnectionState {
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
                Err(e) => {
                    tracing::error!("watch stream error: {:?}", e);
                    continue;
                }
            };
        }
    }
}
