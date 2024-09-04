use kv::KVClient;
use std::{
    sync::{Arc, Mutex, Weak},
    time::Duration,
};
use tonic::Status;

mod ids;
pub mod kv;
pub mod lease;
mod utils;
pub mod watcher;

use lease::{LeaseHandle, LeaseWorkerHandle, LeaseWorkerMessage};
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender, WeakUnboundedSender};
pub use tokio_etcd_grpc_client::ClientEndpointConfig;
use tokio_etcd_grpc_client::EtcdGrpcClient;

pub struct Client {
    grpc_client: EtcdGrpcClient,
    watcher_singleton: WeakSingleton<watcher::WatcherHandle>,
    lease_worker_singleton: WeakUnboundedSenderSingleton<LeaseWorkerMessage>,
}

impl Client {
    pub fn new(
        peers: impl IntoIterator<Item = impl AsRef<str>> + ExactSizeIterator,
        endpoint_config: ClientEndpointConfig,
    ) -> Self {
        let grpc_client = tokio_etcd_grpc_client::client(peers, endpoint_config).unwrap();
        Self {
            grpc_client,
            watcher_singleton: WeakSingleton::new(),
            lease_worker_singleton: WeakUnboundedSenderSingleton::new(),
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

    /// Creates, or returns an existing [`Watcher`] that can be used to watch keys in etcd.
    ///
    /// If there is already an existing watcher, this method will return a clone of it, otherwise
    /// it will create a new watcher. When all references to the watcher are dropped, the watcher will
    /// be dropped as well.
    ///
    /// The watcher coalesces watch requests, so that multiple requests watching the same key will only
    /// result in a single watch request being made to etcd.
    pub fn watcher(&self) -> Arc<watcher::WatcherHandle> {
        self.watcher_singleton.get_or_init(|| {
            watcher::WatcherHandle::new(self.grpc_client.watch(), self.grpc_client.kv())
        })
    }

    /// Acquires a lease with the given TTL, spawning a background worker to continue to keep-alive the lease
    /// as long as the returned [`lease::LeaseHandle`] is alive.
    ///
    /// The lease handle provides methods for checking if the lease is still valid.
    ///
    /// `ttl` must be above 10 seconds.
    pub async fn grant_lease(&self, ttl: Duration) -> Result<LeaseHandle, Status> {
        let sender = self.lease_worker_singleton.get_or_init(|| {
            let handle = LeaseWorkerHandle::spawn(self.grpc_client.lease());
            handle.into_inner()
        });

        LeaseHandle::grant(
            self.grpc_client.lease(),
            LeaseWorkerHandle::from_sender(sender),
            ttl,
        )
        .await
    }

    pub fn kv(&self) -> KVClient {
        KVClient::new(self.grpc_client.kv())
    }
}

/// A weak singleton will only create a new instance of the inner type if there are no other strong
/// references to it. If there are, it will return a strong reference to the existing instance.
struct WeakSingleton<T> {
    // optimization, fixme: we could make this an RwLock.
    inner: Mutex<Weak<T>>,
}

impl<T> WeakSingleton<T> {
    fn new() -> Self {
        Self {
            inner: Mutex::new(Weak::new()),
        }
    }

    fn get_or_init(&self, init: impl FnOnce() -> T) -> Arc<T> {
        let mut lock = self.inner.lock().unwrap();
        if let Some(inner) = lock.upgrade() {
            inner
        } else {
            let arc = Arc::new(init());
            *lock = Arc::downgrade(&arc);
            arc
        }
    }
}

struct WeakUnboundedSenderSingleton<T> {
    inner: Mutex<WeakUnboundedSender<T>>,
}

impl<T> WeakUnboundedSenderSingleton<T> {
    fn new() -> Self {
        // This is kinda awful, but tokio provides no way to create a "weak unbounded sender" without first creating a tx/rx pair.
        let (tx, rx) = unbounded_channel();
        let weak_tx = tx.downgrade();
        drop(rx);

        Self {
            inner: Mutex::new(weak_tx),
        }
    }

    fn get_or_init(&self, init: impl FnOnce() -> UnboundedSender<T>) -> UnboundedSender<T> {
        let mut lock = self.inner.lock().unwrap();
        if let Some(inner) = lock.upgrade() {
            inner
        } else {
            let tx = init();
            let weak_tx = tx.downgrade();

            *lock = weak_tx;
            tx
        }
    }
}
