pub(crate) mod conn_establish;

use std::net::Shutdown;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use anyhow::Context;
use crossbeam_skiplist::SkipMap;
use dashmap::DashMap;
use getset::{CopyGetters, Getters};
use log::{debug, error, info, warn};
use mio::{Token, Waker};
use thiserror::Error;
use atlas_common::{channel, Err};
use atlas_common::channel::{ChannelSyncRx, ChannelSyncTx, OneShotRx, TryRecvError};
use atlas_common::node_id::{NodeId, NodeType};
use atlas_common::socket::{MioSocket, SecureSocket, SecureSocketSync, SyncListener};
use atlas_communication::byte_stub;
use atlas_communication::byte_stub::{ByteNetworkController, NodeIncomingStub, NodeStubController};
use atlas_communication::byte_stub::connections::NetworkConnectionController;
use atlas_communication::message::{NetworkSerializedMessage, WireMessage};
use atlas_communication::reconfiguration::NetworkInformationProvider;
use crate::conn_util::{ConnCounts, ReadingBuffer, WritingBuffer};
use crate::connections::conn_establish::ConnectionHandler;
use crate::connections::conn_establish::pending_conn::{RegisteredServers, ServerRegisteredPendingConns};
use crate::epoll::{EpollWorkerGroupHandle, EpollWorkerId, NewConnection};

pub const SEND_QUEUE_SIZE: usize = 1024;

/// The manager for all currently active connections
#[derive(Getters, CopyGetters)]
pub struct Connections<NI, IS, CNP>
    where NI: NetworkInformationProvider {
    #[get_copy = "pub"]
    own_id: NodeId,

    server_conns: Arc<ServerRegisteredPendingConns>,
    registered_servers: RegisteredServers,
    #[get = "pub(crate)"]
    stub_controller: CNP,
    registered_connections: DashMap<NodeId, Arc<PeerConn<IS>>>,
    #[get = "pub"]
    network_info: Arc<NI>,

    group_worker_handle: EpollWorkerGroupHandle<IS>,

    conn_counts: ConnCounts,

    conn_handle: Arc<ConnectionHandler>,
}

/// A connection to a given peer
#[derive(Getters, CopyGetters)]
pub struct PeerConn<IS> {
    // The peer ID of the connected node
    #[get_copy = "pub(super)"]
    connected_peer_id: NodeId,
    // Node type of the peer connection
    #[get_copy = "pub(super)"]
    node_type: NodeType,

    // The stub to propagate messages to the upper levels of the protocol
    #[get = "pub(super)"]
    byte_input_stub: IS,

    // A thread-safe counter for generating connection ids
    conn_id_generator: AtomicU32,
    //The map connecting each connection to a token in the MIO Workers
    connections: Arc<SkipMap<u32, Option<ConnHandle>>>,
    // Sending messages to the connections
    to_send: (
        ChannelSyncTx<WireMessage>,
        ChannelSyncRx<WireMessage>,
    ),
}


impl<NI, CN, CNP> Connections<NI, CN, CNP>
    where CNP: NodeStubController<ByteMessageSendStub, CN> + 'static,
          NI: NetworkInformationProvider + 'static,
          CN: NodeIncomingStub + 'static {
    pub(super) fn initialize_connections(
        network_info: Arc<NI>,
        group_worker_handle: EpollWorkerGroupHandle<CN>,
        conn_counts: ConnCounts,
        stub_controller: CNP,
    ) -> Self {
        let own_id = network_info.get_own_id();

        let conn_handler = Arc::new(ConnectionHandler::initialize(own_id, conn_counts.clone()));

        let server_conns = Arc::new(ServerRegisteredPendingConns::new());

        let registered_servers = RegisteredServers::init();

        Self {
            own_id,
            server_conns,
            registered_servers,
            stub_controller,
            registered_connections: Default::default(),
            network_info,
            group_worker_handle,
            conn_counts,
            conn_handle: conn_handler,
        }
    }

    pub(super) fn setup_tcp_worker(self: &Arc<Self>, listener: SyncListener) {
        let (tx, rx) = channel::new_bounded_sync(SEND_QUEUE_SIZE, Some("TCP Server Worker"));

        self.registered_servers.register_server(tx);

        let _waker = conn_establish::initialize_server(
            self.own_id.clone(),
            listener,
            self.conn_handle.clone(),
            self.server_conns.clone(),
            self.network_info.clone(),
            Arc::clone(self),
            rx,
        );
    }

    pub(crate) fn get_connection(&self, node: &NodeId) -> Option<Arc<PeerConn<CN>>> {
        self.registered_connections.get(node).map(|entry| entry.value().clone())
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
    fn connect_to_node(
        self: &Arc<Self>,
        node: NodeId,
    ) -> Vec<OneShotRx<atlas_common::error::Result<()>>> {
        if node == self.own_id {
            warn!("Attempted to connect to myself");

            return vec![];
        }
        let addr = self.network_info.get_addr_for_node(&node);
        let node_type = self.network_info.get_node_type(&node);

        if addr.is_none() || node_type.is_none() {
            error!("No address found for node {:?}", node);

            return vec![];
        } else {
            match (self.network_info.get_own_node_type(), node_type.unwrap()) {
                (NodeType::Client, NodeType::Client) => {
                    warn!("Attempted to connect to another client");

                    return vec![];
                }
                _ => {}
            }
        }

        let addr = addr.unwrap();

        let current_connections = self
            .registered_connections
            .get(&node)
            .map(|entry| entry.value().concurrent_connection_count())
            .unwrap_or(0);

        let connections = self
            .conn_counts
            .get_connections_to_node(self.own_id, node, &*self.network_info);

        let connections = if current_connections > connections {
            0
        } else {
            connections - current_connections
        };

        let mut result_vec = Vec::with_capacity(connections);

        for _ in 0..connections {
            result_vec.push(
                self.conn_handle
                    .connect_to_node(Arc::clone(self), node, node_type.unwrap(), addr.clone()),
            )
        }

        result_vec
    }

    fn dc_from_node(&self, node: &NodeId) -> atlas_common::error::Result<()> {
        let existing_connection = self.registered_connections.remove(node);

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
    fn preemptive_conn_register(self: &Arc<Self>, node: NodeId, node_type: NodeType, channel: (ChannelSyncTx<NetworkSerializedMessage>, ChannelSyncRx<NetworkSerializedMessage>))
                                -> atlas_common::error::Result<Arc<PeerConn<CN>>> {
        debug!("Preemptively registering connection to node {:?}", node);

        let option = self.registered_connections.entry(node);

        let conn = option.or_insert_with(move || {
            let connections = Arc::new(SkipMap::new());

            let byte_stub = ByteMessageSendStub(channel.0.clone(), connections.clone());

            let client_reception = self.stub_controller.generate_stub_for(node, node_type, byte_stub).expect("Failed to create reception client");

            Arc::new(PeerConn::init(node, node_type, client_reception, connections, channel))
        });

        Ok(conn.value().clone())
    }

    /// Handle a given socket having established the necessary connection
    fn handle_connection_established(self: &Arc<Self>, node: NodeId,
                                     socket: SecureSocket,
                                     node_type: NodeType,
                                     reading_info: ReadingBuffer,
                                     writing_info: Option<WritingBuffer>,
                                     channel: (ChannelSyncTx<NetworkSerializedMessage>, ChannelSyncRx<NetworkSerializedMessage>)) {
        let socket = match socket {
            SecureSocket::Sync(sync) => match sync {
                SecureSocketSync::Plain(socket) => socket,
                SecureSocketSync::Tls(tls, socket) => socket,
            },
            _ => unreachable!(),
        };

        self.handle_connection_established_with_socket(node, socket.into(), node_type, reading_info, writing_info, channel);
    }

    fn handle_connection_established_with_socket(
        self: &Arc<Self>,
        node: NodeId,
        socket: MioSocket,
        node_type: NodeType,
        reading_info: ReadingBuffer,
        writing_info: Option<WritingBuffer>,
        channel: (ChannelSyncTx<NetworkSerializedMessage>, ChannelSyncRx<NetworkSerializedMessage>),
    ) -> Arc<PeerConn<CN>> {
        info!(
            "{:?} // Handling established connection to {:?} with node type: {:?}",
            self.own_id, node, node_type
        );

        let option = self.registered_connections.entry(node);

        let peer_conn = option.or_insert_with(move || {
            let connections = Arc::new(SkipMap::new());

            let byte_stub = ByteMessageSendStub(channel.0.clone(), connections.clone());

            let reception_client = self.stub_controller.generate_stub_for(node, node_type, byte_stub).expect("Failed to create reception client");

            let con = Arc::new(PeerConn::init(
                node,
                node_type,
                reception_client,
                connections,
                channel,
            ));

            debug!(
                "{:?} // Creating new peer connection to {:?}.",
                self.own_id,
                node,
            );

            con
        });

        let concurrency_level = self.conn_counts
            .get_connections_to_node(self.own_id, node, &*self.network_info);

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

            return peer_conn.value().clone();
        }

        debug!(
            "{:?} // Registering connection {:?} to {:?}",
            self.own_id, conn_id, node
        );

        //FIXME: This isn't really an atomic operation but I also don't care LOL.
        peer_conn.register_peer_conn_intent(conn_id);

        let conn_details =
            NewConnection::new(conn_id, node, self.own_id, socket, reading_info, writing_info, peer_conn.value().clone());

        // We don't register the connection here as we still need some information that will only be provided
        // to us by the worker that will handle the connection.
        // Therefore, the connection will be registered in the worker itself.
        self.group_worker_handle
            .assign_socket_to_worker(conn_details)
            .expect("Failed to assign socket to worker?");

        return peer_conn.value().clone();
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

            let _ = self.connect_to_node(node);
        }
    }
    pub fn pending_server_connections(&self) -> &Arc<ServerRegisteredPendingConns> {
        &self.server_conns
    }

    pub fn registered_servers(&self) -> &RegisteredServers {
        &self.registered_servers
    }
}

impl<NI, IS, CNP> NetworkConnectionController for Connections<NI, IS, CNP>
    where NI: NetworkInformationProvider + 'static,
          IS: NodeIncomingStub + 'static,
          CNP: NodeStubController<ByteMessageSendStub, IS> + 'static {
    fn has_connection(&self, node: &NodeId) -> bool {
        self.is_connected_to_node(node)
    }

    fn currently_connected_node_count(&self) -> usize {
        self.connected_nodes_count()
    }

    fn currently_connected_nodes(&self) -> Vec<NodeId> {
        self.connected_nodes()
    }

    fn connect_to_node(&self, node: NodeId) -> atlas_common::error::Result<()> {
        self.connect_to_node(node);

        Ok(())
    }

    fn disconnect_from_node(&self, node: &NodeId) -> atlas_common::error::Result<()> {
        self.dc_from_node(node)
    }
}


impl<ST> PeerConn<ST> {
    pub fn init(connected_peer_id: NodeId,
                node_type: NodeType,
                byte_input_stub: ST,
                connections: Arc<SkipMap<u32, Option<ConnHandle>>>,
                channel: (ChannelSyncTx<NetworkSerializedMessage>, ChannelSyncRx<NetworkSerializedMessage>)) -> Self {
        Self {
            connected_peer_id,
            node_type,
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
    pub(super) fn take_from_to_send(&self) -> atlas_common::error::Result<NetworkSerializedMessage> {
        self.to_send.1.recv().context("Failed to take message from send queue")
    }

    /// Attempt to take a message from the send queue (non blocking)
    pub(super) fn try_take_from_send(&self) -> atlas_common::error::Result<Option<NetworkSerializedMessage>> {
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
pub struct ByteMessageSendStub(ChannelSyncTx<WireMessage>, Arc<SkipMap<u32, Option<ConnHandle>>>);

impl byte_stub::ByteNetworkStub for ByteMessageSendStub {
    fn dispatch_message(&self, message: WireMessage) -> atlas_common::error::Result<()> {
        self.0.send(message)?;

        self.1.iter().for_each(|entry| {
            if let Some(conn) = entry.value() {
                conn.waker.wake().expect("Failed to wake connection");
            }
        });

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
    FailedToRetrieveFromSendQueue
}