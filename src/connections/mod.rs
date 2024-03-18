pub(crate) mod conn_establish;

use crate::conn_util;
use crate::conn_util::{ConnCounts, ReadingBuffer, WritingBuffer};
use crate::connections::conn_establish::{ConnectionEstablishError, ConnectionHandler};
use crate::epoll::{EpollWorkerGroupHandle, EpollWorkerId, NewConnection};
use anyhow::{anyhow, Context};
use atlas_common::channel::{ChannelSyncRx, ChannelSyncTx, OneShotRx, TryRecvError};
use atlas_common::node_id::{NodeId, NodeType};
use atlas_common::socket::{MioSocket, SecureSocket, SecureSocketSync, SyncListener};
use atlas_common::{channel, Err};
use atlas_communication::byte_stub;
use atlas_communication::byte_stub::connections::NetworkConnectionController;
use atlas_communication::byte_stub::{ByteNetworkController, NodeIncomingStub, NodeStubController};
use atlas_communication::message::{NetworkSerializedMessage, WireMessage};
use atlas_communication::reconfiguration::{NetworkInformationProvider, NodeInfo};
use crossbeam_skiplist::SkipMap;
use dashmap::mapref::entry::Entry;
use dashmap::mapref::one::Ref;
use dashmap::DashMap;
use getset::{CopyGetters, Getters};
use log::{debug, error, info, warn};
use mio::{Token, Waker};
use std::net::Shutdown;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use thiserror::Error;

pub const SEND_QUEUE_SIZE: usize = 1024;

/// The manager for all currently active connections
#[derive(Getters, CopyGetters)]
pub struct Connections<NI, IS, CNP>
where
    NI: NetworkInformationProvider,
{
    #[get_copy = "pub"]
    own_id: NodeId,

    #[get = "pub(crate)"]
    stub_controller: CNP,
    registered_connections: DashMap<NodeId, Arc<PeerConn<IS>>>,
    #[get = "pub"]
    network_info: Arc<NI>,

    group_worker_handle: EpollWorkerGroupHandle<IS>,

    conn_counts: ConnCounts,
    #[get = "pub"]
    loopback: IS,

    conn_handle: Arc<ConnectionHandler>,
}

/// A connection to a given peer
#[derive(Getters, CopyGetters)]
pub struct PeerConn<IS> {
    // The peer ID of the connected node
    #[get_copy = "pub(super)"]
    connected_peer_id: NodeId,
    // The stub to propagate messages to the upper levels of the protocol
    #[get = "pub(super)"]
    byte_input_stub: IS,

    // A thread-safe counter for generating connection ids
    conn_id_generator: AtomicU32,
    //The map connecting each connection to a token in the MIO Workers
    connections: Arc<SkipMap<u32, Option<ConnHandle>>>,
    // Sending messages to the connections
    to_send: (ChannelSyncTx<WireMessage>, ChannelSyncRx<WireMessage>),
}

impl<NI, CN, CNP> Connections<NI, CN, CNP>
where
    CNP: NodeStubController<ByteMessageSendStub, CN> + 'static,
    NI: NetworkInformationProvider + 'static,
    CN: NodeIncomingStub + 'static,
{
    pub(super) fn initialize_connections(
        network_info: Arc<NI>,
        group_worker_handle: EpollWorkerGroupHandle<CN>,
        conn_counts: ConnCounts,
        stub_controller: CNP,
    ) -> Self {
        let own_id = network_info.own_node_info().node_id();

        let conn_handler = Arc::new(ConnectionHandler::initialize(own_id, conn_counts.clone()));

        let loopback = stub_controller
            .get_stub_for(&own_id)
            .expect("Failed to get loopback stub");

        Self {
            own_id,
            stub_controller,
            registered_connections: Default::default(),
            network_info,
            group_worker_handle,
            conn_counts,
            loopback,
            conn_handle: conn_handler,
        }
    }

    pub(super) fn setup_tcp_worker(self: &Arc<Self>, listener: SyncListener) {
        let _waker = conn_establish::initialize_server(
            self.own_id.clone(),
            listener,
            self.conn_handle.clone(),
            self.network_info.clone(),
            Arc::clone(self),
        );
    }

    pub(crate) fn get_connection(&self, node: &NodeId) -> Option<Arc<PeerConn<CN>>> {
        self.registered_connections
            .get(node)
            .map(|entry| entry.value().clone())
    }

    fn is_connected_to_node(&self, node: &NodeId) -> bool {
        self.registered_connections.contains_key(node)
    }

    fn connected_nodes_count(&self) -> usize {
        self.registered_connections.len()
    }

    fn connected_nodes(&self) -> Vec<NodeId> {
        self.registered_connections
            .iter()
            .map(|entry| entry.key().clone())
            .collect()
    }

    /// Attempt to connect to a given node
    fn internal_connect_to_node(
        self: &Arc<Self>,
        node: NodeId,
    ) -> Result<Vec<OneShotRx<atlas_common::error::Result<()>>>, ConnectionError> {
        if node == self.own_id {
            return Err!(ConnectionError::ConnectToSelf);
        }

        let node_info = self.network_info.get_node_info(&node);

        let node_info = if node_info.is_none() {
            return Err!(ConnectionError::NodeInfoNotFound(node));
        } else {
            match (
                self.network_info.own_node_info().node_type(),
                node_info.clone().unwrap().node_type(),
            ) {
                (NodeType::Client, NodeType::Client) => {
                    return Err!(ConnectionError::ClientCannotConnectToClient(node));
                }
                _ => node_info.unwrap(),
            }
        };

        let current_connections = self
            .registered_connections
            .get(&node)
            .map(|entry| entry.value().concurrent_connection_count())
            .unwrap_or(0);

        let target_connections = self.conn_counts.get_connections_to_node(
            self.network_info.own_node_info().node_type(),
            node_info.node_type(),
        );

        let connections = if current_connections > target_connections {
            0
        } else {
            target_connections - current_connections
        };

        let mut result_vec = Vec::with_capacity(connections);

        for _ in 0..connections {
            result_vec.push(self.conn_handle.connect_to_node(
                Arc::clone(self),
                node,
                node_info.addr().clone(),
            )?)
        }

        Ok(result_vec)
    }

    fn dc_from_node(&self, node: &NodeId) -> atlas_common::error::Result<()> {
        let existing_connection = self.registered_connections.remove(node);

        self.stub_controller.shutdown_stubs_for(node);

        if let Some((node, connection)) = existing_connection {
            for entry in connection.connections.iter() {
                if let Some(conn) = entry.value() {
                    let worker_id = conn.epoll_worker_id;
                    let conn_token = conn.token;

                    self.group_worker_handle
                        .disconnect_connection_from_worker(worker_id, conn_token)?;
                }
            }
        }

        Ok(())
    }

    /// Register a connection without having to provide any sockets, as this is meant to be done
    /// preemptively so there is no possibility for the connection details to be lost due to
    /// multi threading non atomic shenanigans
    fn preemptive_conn_register(
        self: &Arc<Self>,
        node: NodeId,
        channel: (
            ChannelSyncTx<NetworkSerializedMessage>,
            ChannelSyncRx<NetworkSerializedMessage>,
        ),
    ) -> atlas_common::error::Result<Arc<PeerConn<CN>>> {
        debug!("Preemptively registering connection to node {:?}", node);

        let option = self.registered_connections.entry(node);

        let conn = option.or_insert_with(move || {
            let connections = Arc::new(SkipMap::new());

            let byte_stub = ByteMessageSendStub(channel.0.clone(), connections.clone());

            let client_reception = self
                .stub_controller
                .generate_stub_for(node, byte_stub)
                .expect("Failed to create reception client");

            Arc::new(PeerConn::init(node, client_reception, connections, channel))
        });

        Ok(conn.value().clone())
    }

    /// Handle a given socket having established the necessary connection
    fn handle_connection_established(
        self: &Arc<Self>,
        node: NodeInfo,
        socket: SecureSocket,
        reading_info: ReadingBuffer,
        writing_info: Option<WritingBuffer>,
    ) -> atlas_common::error::Result<()> {
        let socket = match socket {
            SecureSocket::Sync(sync) => match sync {
                SecureSocketSync::Plain(socket) => socket,
                SecureSocketSync::Tls(tls, socket) => socket,
            },
            _ => unreachable!(),
        };

        let to_send_channel = match self.registered_connections.get(&node.node_id()) {
            None => conn_util::initialize_send_channel(),
            Some(conn) => conn.to_send.clone(),
        };

        // Cannot call this function while holding a reference to the registered connections map
        self.handle_connection_established_with_socket(
            node,
            socket.into(),
            reading_info,
            writing_info,
            to_send_channel,
        )?;

        Ok(())
    }

    fn handle_connection_established_with_socket(
        self: &Arc<Self>,
        node: NodeInfo,
        socket: MioSocket,
        reading_info: ReadingBuffer,
        writing_info: Option<WritingBuffer>,
        channel: (
            ChannelSyncTx<NetworkSerializedMessage>,
            ChannelSyncRx<NetworkSerializedMessage>,
        ),
    ) -> atlas_common::error::Result<Arc<PeerConn<CN>>> {
        info!(
            "{:?} // Handling established connection to {:?}",
            self.own_id, node
        );

        let other_node = node.clone();

        let peer_conn = match self.registered_connections.entry(node.node_id()) {
            Entry::Occupied(conn) => conn.get().clone(),
            Entry::Vacant(vacant) => {
                let connections = Arc::new(SkipMap::new());

                let stub = self.stub_controller.get_stub_for(&node.node_id());

                let stub = match stub {
                    None => {
                        let byte_stub = ByteMessageSendStub(channel.0.clone(), connections.clone());

                        self.stub_controller
                            .generate_stub_for(node.node_id(), byte_stub)?
                    }
                    Some(_) => {
                        unreachable!("We should never have a stub for a node that we don't have a connection to")
                    }
                };

                let con = Arc::new(PeerConn::init(
                    other_node.node_id(),
                    stub,
                    connections,
                    channel,
                ));

                debug!(
                    "{:?} // Creating new peer connection to {:?}.",
                    self.own_id, other_node,
                );

                vacant.insert(con.clone());

                con
            }
        };

        let concurrency_level = self.conn_counts.get_connections_to_node(
            self.network_info.own_node_info().node_type(),
            node.node_type(),
        );

        let conn_id = peer_conn.gen_conn_id();

        let current_connections = peer_conn.concurrent_connection_count();

        //FIXME: Fix the fact that we are closing the previous connection when we don't actually need to
        // So now we have to multiply the limit because of this
        if current_connections + 1 > concurrency_level * 2 {
            // We have too many connections to this node. We need to close this one.
            warn!("{:?} // Too many connections to {:?}. Closing connection {:?}. Connection count {} vs max {}", self.own_id, node, conn_id,
            current_connections, concurrency_level);

            if let Err(err) = socket.shutdown(Shutdown::Both) {
                error!(
                    "{:?} // Failed to shutdown socket {:?} to {:?}. Error: {:?}",
                    self.own_id, conn_id, node, err
                );
            }

            return Ok(peer_conn.clone());
        }

        debug!(
            "{:?} // Registering connection {:?} to {:?}",
            self.own_id, conn_id, node
        );

        //FIXME: This isn't really an atomic operation but I also don't care LOL.
        peer_conn.register_peer_conn_intent(conn_id);

        let conn_details = NewConnection::new(
            conn_id,
            node.node_id(),
            self.own_id,
            socket,
            reading_info,
            writing_info,
            peer_conn.clone(),
        );

        // We don't register the connection here as we still need some information that will only be provided
        // to us by the worker that will handle the connection.
        // Therefore, the connection will be registered in the worker itself.
        self.group_worker_handle
            .assign_socket_to_worker(conn_details)?;

        return Ok(peer_conn.clone());
    }

    /// Handle a connection having broken and being removed from the worker
    pub(super) fn handle_connection_failed(self: &Arc<Self>, node: NodeId, conn_id: u32) {
        info!(
            "{:?} // Handling failed connection to {:?}. Conn: {:?}",
            self.own_id, node, conn_id
        );

        let connection = if let Some(conn) = self.registered_connections.get(&node) {
            conn.value().clone()
        } else {
            return;
        };

        connection.delete_connection(conn_id);

        if connection.concurrent_connection_count() == 0 {
            self.registered_connections.remove(&node);

            self.stub_controller.shutdown_stubs_for(&node);

            let _ = self.internal_connect_to_node(node);
        }
    }
}

impl<NI, IS, CNP> NetworkConnectionController for Connections<NI, IS, CNP>
where
    NI: NetworkInformationProvider + 'static,
    IS: NodeIncomingStub + 'static,
    CNP: NodeStubController<ByteMessageSendStub, IS> + 'static,
{
    fn has_connection(&self, node: &NodeId) -> bool {
        self.is_connected_to_node(node)
    }

    fn currently_connected_node_count(&self) -> usize {
        self.connected_nodes_count()
    }

    fn currently_connected_nodes(&self) -> Vec<NodeId> {
        self.connected_nodes()
    }

    fn connect_to_node(
        self: &Arc<Self>,
        node: NodeId,
    ) -> atlas_common::error::Result<Vec<OneShotRx<atlas_common::error::Result<()>>>> {
        let conn_results = self.internal_connect_to_node(node)?;

        Ok(conn_results)
    }

    fn disconnect_from_node(self: &Arc<Self>, node: &NodeId) -> atlas_common::error::Result<()> {
        self.dc_from_node(node)
    }
}

impl<ST> PeerConn<ST> {
    pub fn init(
        connected_peer_id: NodeId,
        byte_input_stub: ST,
        connections: Arc<SkipMap<u32, Option<ConnHandle>>>,
        channel: (
            ChannelSyncTx<NetworkSerializedMessage>,
            ChannelSyncRx<NetworkSerializedMessage>,
        ),
    ) -> Self {
        Self {
            connected_peer_id,
            byte_input_stub,
            conn_id_generator: Default::default(),
            connections,
            to_send: channel,
        }
    }

    /// Get a unique ID for a connection
    fn gen_conn_id(&self) -> u32 {
        self.conn_id_generator.fetch_add(1, Ordering::Relaxed)
    }

    /// Register an active connection into this connection map
    pub(super) fn register_peer_conn(&self, conn: ConnHandle) {
        self.connections.insert(conn.id, Some(conn));
    }

    // Register an intent of registering this connection
    fn register_peer_conn_intent(&self, id: u32) {
        self.connections.insert(id, None);
    }

    /// Get the amount of concurrent connections we currently have to this peer
    fn concurrent_connection_count(&self) -> usize {
        self.connections.len()
    }

    /// Take a message from the send queue (blocking)
    pub(super) fn take_from_to_send(
        &self,
    ) -> atlas_common::error::Result<NetworkSerializedMessage> {
        self.to_send
            .1
            .recv()
            .context("Failed to take message from send queue")
    }

    /// Attempt to take a message from the send queue (non blocking)
    pub(super) fn try_take_from_send(
        &self,
    ) -> atlas_common::error::Result<Option<NetworkSerializedMessage>> {
        match self.to_send.1.try_recv() {
            Ok(msg) => Ok(Some(msg)),
            Err(err) => match err {
                TryRecvError::ChannelDc => Err!(MioError::FailedToRetrieveFromSendQueue),
                TryRecvError::ChannelEmpty | TryRecvError::Timeout => Ok(None),
            },
        }
    }

    pub(super) fn delete_connection(&self, conn_id: u32) {
        self.connections.remove(&conn_id);
    }

    pub fn stub_peer(&self) -> &ST {
        &self.byte_input_stub
    }
}

#[derive(Clone, Getters, CopyGetters)]
pub struct ConnHandle {
    #[get_copy = "pub"]
    id: u32,
    #[get_copy = "pub"]
    my_id: NodeId,
    #[get_copy = "pub"]
    peer_id: NodeId,
    #[get_copy = "pub"]
    epoll_worker_id: EpollWorkerId,
    token: Token,
    waker: Arc<Waker>,
    #[get = "pub"]
    pub(crate) cancelled: Arc<AtomicBool>,
}

/// A handle to a connection that is being established
///
pub struct ByteMessageSendStub(
    ChannelSyncTx<WireMessage>,
    Arc<SkipMap<u32, Option<ConnHandle>>>,
);

impl byte_stub::ByteNetworkStub for ByteMessageSendStub {
    fn dispatch_message(&self, message: WireMessage) -> atlas_common::error::Result<()> {
        self.0.send(message).context("Failed to send to channel")?;

        for entry in self.1.iter() {
            if let Some(conn) = entry.value() {
                conn.waker.wake().context("Failed to wake worker")?;
            }
        }

        Ok(())
    }
}

impl Clone for ByteMessageSendStub {
    fn clone(&self) -> Self {
        ByteMessageSendStub(self.0.clone(), self.1.clone())
    }
}

impl ConnHandle {
    pub fn new(
        id: u32,
        my_id: NodeId,
        peer_id: NodeId,
        epoll_worker: EpollWorkerId,
        conn_token: Token,
        waker: Arc<Waker>,
    ) -> Self {
        Self {
            id,
            my_id,
            peer_id,
            epoll_worker_id: epoll_worker,
            cancelled: Arc::new(AtomicBool::new(false)),
            waker,
            token: conn_token,
        }
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Relaxed)
    }
}

#[derive(Error, Debug)]
pub enum MioError {
    #[error("Failed to retrieve message from the send queue")]
    FailedToRetrieveFromSendQueue,
}

#[derive(Error, Debug)]
pub enum ConnectionError {
    #[error("Cannot connect to ourselves")]
    ConnectToSelf,
    #[error("Node info for node {0:?} is not found")]
    NodeInfoNotFound(NodeId),
    #[error("Failed to connect to node {0:?} as we are both clients")]
    ClientCannotConnectToClient(NodeId),
    #[error("Failed to connect to node due to internal error {0:?}")]
    InternalConnectionError(#[from] ConnectionEstablishError),
}
