use std::sync::OnceLock;

mod watcher;

pub use tokio_etcd_grpc_client::ClientEndpointConfig;
use tokio_etcd_grpc_client::EtcdGrpcClient;

pub use watcher::{Watcher, WatcherKey};

pub struct Client {
    grpc_client: EtcdGrpcClient,
    watcher_singleton: OnceLock<Watcher>,
}

impl Client {
    pub fn new(
        peers: impl IntoIterator<Item = impl AsRef<str>> + ExactSizeIterator,
        endpoint_config: ClientEndpointConfig,
    ) -> Self {
        let grpc_client = tokio_etcd_grpc_client::client(peers, endpoint_config).unwrap();
        Self {
            grpc_client,
            watcher_singleton: OnceLock::new(),
        }
    }

    /// Updates the authentication token used by the client.
    ///
    /// This token will be used for all future requests made by the client. This method can be used to do live
    /// token rotation, without having to create a new client.
    pub fn set_auth_token(&self, token: http::HeaderValue) {
        self.grpc_client.set_auth_token(token);
    }

    /// Clears the authentication token used by the client.
    ///
    /// This will cause the client to make requests without an authentication token.
    pub fn clear_auth_token(&self) {
        self.grpc_client.clear_auth_token();
    }

    pub fn watcher(&self) -> Watcher {
        self.watcher_singleton
            .get_or_init(|| Watcher::new(self.grpc_client.watch(), self.grpc_client.kv()))
            .clone()
    }
}
