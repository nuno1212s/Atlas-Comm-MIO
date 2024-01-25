use std::net::SocketAddr;
use std::sync::Arc;
use anyhow::Context;
use atlas_common::error::*;
use atlas_common::node_id::NodeId;
use atlas_common::socket;
use atlas_common::socket::SyncListener;
use atlas_communication::byte_stub::{ByteNetworkController, ByteNetworkStub, NodeIncomingStub, NodeStubController};
use atlas_communication::reconfiguration_node::{NetworkInformationProvider, ReconfigurationMessageHandler};
use crate::config::MIOConfig;
use crate::conn_util::ConnCounts;
use crate::connections::{ByteMessageSendStub, Connections};
use crate::connections::conn_establish::pending_conn::NetworkUpdateHandler;
use crate::epoll::{init_worker_group_handle, initialize_worker_group};

mod connections;
mod epoll;
pub(crate) mod conn_util;
mod config;

/// The byte level TCP MIO epoll based module
/// Utilizes the MIO library to provide a TCP based communication layer
/// much faster than the existing async std or tokio based options, due to the much lower overhead
/// of (not) changing the context of the execution
pub struct MIOTCPNode<NI, IS, CNP>
    where NI: NetworkInformationProvider,
          CNP: Clone {
    network_information: Arc<NI>,
    stub_controller: CNP,
    connections: Arc<Connections<NI, IS, CNP>>,
    reconfig_msg_handle: Arc<ReconfigurationMessageHandler>,
}

impl<NI, IS, CNP> MIOTCPNode<NI, IS, CNP> where CNP: Clone, NI: NetworkInformationProvider {
    fn setup_connection(id: &NodeId, server_addr: &SocketAddr) -> Result<SyncListener> {
        socket::bind_sync_server(server_addr.clone()).context(format!("Failed to setup connection with socket {:?}", server_addr))
    }
}

impl<NI, IS, CNP> ByteNetworkController<NI, CNP, ByteMessageSendStub, IS> for MIOTCPNode<NI, IS, CNP>
    where NI: NetworkInformationProvider + 'static,
          CNP: NodeStubController<ByteMessageSendStub, IS> + 'static,
          IS: NodeIncomingStub + 'static {
    type Config = MIOConfig;

    type ConnectionController = Connections<NI, IS, CNP>;

    fn initialize_controller(network_info: Arc<NI>, config: Self::Config, stub_controllers: CNP) -> Result<Self>
        where Self: Sized {
        let reconfig_message_handler = Arc::new(ReconfigurationMessageHandler::initialize());

        let (handle, receivers) = init_worker_group_handle::<NI, IS, CNP>(config.epoll_worker_count());

        let connections = Arc::new(Connections::initialize_connections(
            network_info.clone(),
            handle,
            ConnCounts::from_tcp_config(config.tcp_configs()),
            stub_controllers.clone(),
        ));

        NetworkUpdateHandler::initialize_update_handler(
            connections.registered_servers().clone(),
            connections.pending_server_connections().clone(),
            reconfig_message_handler.update_channel_rx().clone(),
            connections.clone(),
        );

        initialize_worker_group(connections.clone(), receivers)?;

        let addr = network_info.get_own_addr();

        let id = network_info.get_own_id();

        let listener = Self::setup_connection(&id, addr.socket())?;

        connections.setup_tcp_worker(listener);

        Ok(Self {
            network_information: network_info,
            stub_controller: stub_controllers,
            connections,
            reconfig_msg_handle: reconfig_message_handler,
        })
    }

    /// The controller of the connections
    fn connection_controller(&self) -> &Arc<Self::ConnectionController> {
        &self.connections
    }
}

impl<NI, IS, CNP> Clone for MIOTCPNode<NI, IS, CNP>
    where NI: NetworkInformationProvider, CNP: Clone {
    fn clone(&self) -> Self {
        Self {
            network_information: self.network_information.clone(),
            stub_controller: self.stub_controller.clone(),
            connections: self.connections.clone(),
            reconfig_msg_handle: self.reconfig_msg_handle.clone(),
        }
    }
}