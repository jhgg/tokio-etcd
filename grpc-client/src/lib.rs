mod auth_service;
mod pb;

use core::fmt;
use std::{
    fmt::{Display, Formatter},
    sync::Arc,
    time::Duration,
};

use auth_service::AuthService;

use http::{uri::InvalidUri, HeaderValue};
use tonic::transport::{Channel, Endpoint};
use tower::discover::Change;

pub use auth_service::AuthServiceTokenSetter;
pub use pb::{
    etcdserverpb::{
        auth_client::AuthClient, cluster_client::ClusterClient, kv_client::KvClient,
        lease_client::LeaseClient, maintenance_client::MaintenanceClient,
        watch_client::WatchClient, *,
    },
    mvccpb::{event::EventType, Event, KeyValue},
};

pub type AuthedChannel = AuthService<Channel>;

#[derive(Debug, Default)]
pub enum EndpointSchema {
    #[default]
    Http,
    // Https,
}
impl EndpointSchema {
    fn default_port(&self) -> u16 {
        match self {
            EndpointSchema::Http => 2379,
            // EndpointSchema::Https => ????,
        }
    }
}

impl Display for EndpointSchema {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            EndpointSchema::Http => write!(f, "http"),
            // EndpointSchema::Https => write!(f, "https"),
        }
    }
}

#[derive(Debug, Default)]
pub enum EndpointPort {
    #[default]
    DefaultForSchema,
    Custom(u16),
}

#[derive(Default, Debug)]
pub struct ClientEndpointConfig {
    schema: EndpointSchema,
    port: EndpointPort,
    token: Option<HeaderValue>,
    request_timeout: Duration,
    connect_timeout: Duration,
    tcp_keep_alive: Option<Duration>,
    http2_keep_alive_interval: Duration,
    http2_keep_alive_timeout: Duration,
    http2_keep_alive_while_idle: bool,
}

impl ClientEndpointConfig {
    const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
    const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
    const DEFAULT_HTTP2_KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(30);
    const DEFAULT_HTTP2_KEEP_ALIVE_TIMEOUT: Duration = Duration::from_secs(15);

    pub fn http() -> Self {
        Self {
            schema: EndpointSchema::Http,
            connect_timeout: Self::DEFAULT_CONNECT_TIMEOUT,
            request_timeout: Self::DEFAULT_REQUEST_TIMEOUT,
            http2_keep_alive_interval: Self::DEFAULT_HTTP2_KEEP_ALIVE_INTERVAL,
            http2_keep_alive_timeout: Self::DEFAULT_HTTP2_KEEP_ALIVE_TIMEOUT,
            ..Default::default()
        }
    }

    pub fn auth_token(mut self, token: HeaderValue) -> Self {
        self.token = Some(token);
        self
    }

    /// Default: 5s.
    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }

    /// Default: 3s.
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    /// Default: None.
    pub fn tcp_keep_alive(mut self, timeout: Duration) -> Self {
        self.tcp_keep_alive = Some(timeout);
        self
    }

    /// Default: 30s
    pub fn http2_keep_alive_interval(mut self, interval: Duration) -> Self {
        self.http2_keep_alive_interval = interval;
        self
    }

    /// Defualt: 15s
    pub fn http2_keep_alive_timeout(mut self, timeout: Duration) -> Self {
        self.http2_keep_alive_timeout = timeout;
        self
    }

    fn configure(&self, endpoint: Endpoint) -> Endpoint {
        endpoint
            .timeout(self.request_timeout)
            .connect_timeout(self.connect_timeout)
            .tcp_keepalive(self.tcp_keep_alive)
            .http2_keep_alive_interval(self.http2_keep_alive_interval)
            .keep_alive_while_idle(self.http2_keep_alive_while_idle)
            .keep_alive_timeout(self.http2_keep_alive_timeout)
            .user_agent(concat!(
                env!("CARGO_PKG_NAME"),
                "/",
                env!("CARGO_PKG_VERSION")
            ))
            .expect("invariant: user-agent should always be valid")
    }

    fn port(&self) -> u16 {
        match self.port {
            EndpointPort::DefaultForSchema => self.schema.default_port(),
            EndpointPort::Custom(port) => port,
        }
    }
}

// todo: better name for this?
struct InnerClients {
    // This order is as defined in the proto, so we're keeping it.
    kv: KvClient<AuthedChannel>,
    watch: WatchClient<AuthedChannel>,
    lease: LeaseClient<AuthedChannel>,
    cluster: ClusterClient<AuthedChannel>,
    maintenance: MaintenanceClient<AuthedChannel>,
    auth: AuthClient<AuthedChannel>,

    // extras:
    token_setter: AuthServiceTokenSetter,
}

#[derive(Clone)]
pub struct EtcdGrpcClient {
    // Client tries to be extremely cheap to clone by only having 1 arc inside of it, rather than,
    // having the arc of each kvclient, watchclient, etc.
    inner: Arc<InnerClients>,
}

impl InnerClients {
    fn new(channel: AuthedChannel, token_setter: AuthServiceTokenSetter) -> Self {
        Self {
            kv: KvClient::new(channel.clone()),
            watch: WatchClient::new(channel.clone()),
            lease: LeaseClient::new(channel.clone()),
            cluster: ClusterClient::new(channel.clone()),
            maintenance: MaintenanceClient::new(channel.clone()),
            auth: AuthClient::new(channel),
            token_setter,
        }
    }
}

impl EtcdGrpcClient {
    pub fn set_auth_token(&self, token: http::HeaderValue) {
        self.inner.token_setter.set_token(token);
    }

    pub fn clear_auth_token(&self) {
        self.inner.token_setter.clear_token();
    }

    pub fn kv(&self) -> KvClient<AuthedChannel> {
        self.inner.kv.clone()
    }

    pub fn watch(&self) -> WatchClient<AuthedChannel> {
        self.inner.watch.clone()
    }

    pub fn lease(&self) -> LeaseClient<AuthedChannel> {
        self.inner.lease.clone()
    }

    pub fn cluster(&self) -> ClusterClient<AuthedChannel> {
        self.inner.cluster.clone()
    }

    pub fn maintenance(&self) -> MaintenanceClient<AuthedChannel> {
        self.inner.maintenance.clone()
    }

    pub fn auth(&self) -> AuthClient<AuthedChannel> {
        self.inner.auth.clone()
    }
}

/// Create a gRPC client [`Channel`] from a list of etcd endpoints.
pub fn client(
    hostnames: impl IntoIterator<Item = impl AsRef<str>> + ExactSizeIterator,
    endpoint_config: ClientEndpointConfig,
) -> Result<EtcdGrpcClient, InvalidUri> {
    let (channel, tx) = Channel::balance_channel(hostnames.len());
    for hostname in hostnames.into_iter() {
        let endpoint = endpoint_for_hostname(hostname.as_ref(), &endpoint_config)?;
        tx.try_send(Change::Insert(endpoint.uri().clone(), endpoint))
            .expect("invariant: sending on channel cannot fail, as capacity is same as number of endpoints");
    }

    let (channel, token_setter) = AuthService::pair(channel, endpoint_config.token);
    Ok(EtcdGrpcClient {
        inner: Arc::new(InnerClients::new(channel, token_setter)),
    })
}

fn endpoint_for_hostname(
    hostname: &str,
    cfg: &ClientEndpointConfig,
) -> Result<Endpoint, InvalidUri> {
    let url = format!("{}://{hostname}:{}", cfg.schema, cfg.port());
    Ok(cfg.configure(Channel::builder(url.parse()?)))
}
