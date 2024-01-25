use std::sync::Arc;
use atlas_communication::byte_stub::{ByteNetworkController, ByteNetworkStub, NodeIncomingStub, NodeStubController};
use atlas_communication::reconfiguration_node::NetworkInformationProvider;
use crate::config::MIOConfig;
use crate::conn_util::ConnCounts;
use crate::connections::{ByteMessageSendStub, Connections};
use crate::epoll::init_worker_group_handle;

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
}

impl<NI, IS, CNP> ByteNetworkController<NI, CNP, ByteMessageSendStub, IS> for MIOTCPNode<NI, IS, CNP>
    where NI: NetworkInformationProvider + 'static,
          CNP: NodeStubController<ByteMessageSendStub, IS> + 'static,
          IS: NodeIncomingStub + 'static {
    type Config = MIOConfig;

    type ConnectionController = Connections<NI, IS, CNP>;

    fn initialize_controller(network_info: Arc<NI>, config: Self::Config, stub_controllers: CNP) -> Self
        where Self: Sized {

        let (handle, receivers) = init_worker_group_handle::<NI, IS, CNP>(config.epoll_worker_count());

        let connections = Arc::new(Connections::initialize_connections(
            network_info.clone(),
            handle,
            ConnCounts::from_tcp_config(config.tcp_configs()),
            stub_controllers.clone(),
        ));

        Self {
            network_information: network_info,
            stub_controller: stub_controllers,
            connections,
        }
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
        }
    }
}