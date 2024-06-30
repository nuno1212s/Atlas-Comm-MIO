use std::net::SocketAddr;
use getset::{CopyGetters, Getters};
use rustls::{ClientConfig, ServerConfig};

/// The MIO configuration, necessary to initialize the MIO based TCP module
#[derive(Getters, CopyGetters)]
pub struct MIOConfig {
    #[get_copy = "pub"]
    pub epoll_worker_count: u32,
    #[get = "pub"]
    pub tcp_configs: TcpConfig,
}

/// The TLS config struct
pub struct TlsConfig {
    /// The TLS configuration used to connect to replica nodes. (from client nodes)
    pub async_client_config: ClientConfig,
    /// The TLS configuration used to accept connections from client nodes.
    pub async_server_config: ServerConfig,
    ///The TLS configuration used to accept connections from replica nodes (Synchronously)
    pub sync_server_config: ServerConfig,
    ///The TLS configuration used to connect to replica nodes (from replica nodes) (Synchronousy)
    pub sync_client_config: ClientConfig,
}

#[derive(Getters, CopyGetters)]
pub struct TcpConfig {
    /// Addresses to bind to
    #[get = "pub"]
    pub bind_addrs: Option<Vec<SocketAddr>>,
    /// Configurations specific to the networking
    pub network_config: TlsConfig,
    /// How many concurrent connections should be established between replica nodes of the system
    pub replica_concurrent_connections: usize,
    /// How many client concurrent connections should be established between replica <-> client connections
    pub client_concurrent_connections: usize,
}
