use std::{future::pending, pin::Pin, time::Duration};

use futures::future::BoxFuture;
use tokio::{
    sync::mpsc::{unbounded_channel, UnboundedSender},
    time::Sleep,
};
use tokio_etcd_grpc_client::{AuthedChannel, WatchClient, WatchRequest, WatchResponse};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::{Response, Status, Streaming};

use crate::{utils::backoff::ExponentialBackoff, WatchId};

use super::{
    fsm::{ProcessedWatchResponse, WatchConfig, WatcherFsm},
    Key,
};

pub(crate) struct WatcherFsmClient {
    watch_client: WatchClient<AuthedChannel>,
    connection_state: ConnectionState,
    watcher_fsm: WatcherFsm,
    connection_incarnation: u64,
    backoff: ExponentialBackoff,
}

impl WatcherFsmClient {
    pub fn new(watch_client: WatchClient<AuthedChannel>, concurrent_sync_limit: usize) -> Self {
        Self {
            watch_client,
            watcher_fsm: WatcherFsm::new(concurrent_sync_limit),
            connection_state: ConnectionState::Disconnected,
            connection_incarnation: 0,
            backoff: ExponentialBackoff::new(Duration::from_secs(1), Duration::from_secs(10)),
        }
    }

    pub fn add_watcher(&mut self, watch_config: WatchConfig) -> WatchId {
        let watch_id = self.do_fsm_action(|fsm| fsm.add_watcher(watch_config));
        tracing::info!("added watcher: {:?}", watch_id);

        watch_id
    }

    pub fn cancel_watcher(&mut self, watch_id: WatchId) -> Option<WatchConfig> {
        let config = self.do_fsm_action(|fsm| fsm.cancel_watcher(watch_id))?;
        tracing::info!(
            "cancelled watcher, key: {:?}, watch_id: {:?}",
            config.key(),
            watch_id
        );

        Some(config)
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
                        let connected = ConnectedWatcherStream::new(response.into_inner(), sender);
                        tracing::info!(
                            "connected to etcd successfully, incarnation: {}",
                            self.connection_incarnation
                        );
                        self.connection_state = ConnectionState::Connected(connected);
                        self.backoff.succeed();
                    }
                    (Err(error), _) => {
                        let reconnect_delay = self.backoff.fail();
                        tracing::error!(
                            "failed to connect to etcd: {:?}, incarnation: {}. will reconnect in {:?}.",
                            error,
                            self.connection_incarnation,
                            reconnect_delay
                        );
                        self.connection_state = ConnectionState::Reconnecting(Box::pin(
                            tokio::time::sleep(reconnect_delay),
                        ));
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
                        tracing::warn!(
                            "disconnected from etcd reason: {:?}, incarnation: {}. will reconnect in {:?}",
                            disconnect_reason,
                            self.connection_incarnation,
                            reconnect_delay
                        );
                        self.connection_state = ConnectionState::Reconnecting(Box::pin(
                            tokio::time::sleep(reconnect_delay),
                        ));
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
        BoxFuture<
            'static,
            (
                Result<Response<Streaming<WatchResponse>>, Status>,
                UnboundedSender<WatchRequest>,
            ),
        >,
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
}

#[derive(Debug)]
enum DisconnectReason {
    StreamEnded,
    SenderClosed,
}

impl ConnectedWatcherStream {
    fn new(stream: Streaming<WatchResponse>, sender: UnboundedSender<WatchRequest>) -> Self {
        ConnectedWatcherStream { stream, sender }
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
            };

            match message {
                Ok(Some(response)) => {
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
