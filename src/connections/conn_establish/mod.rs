#![allow(clippy::large_enum_variant)]

use anyhow::{anyhow, Context};
use std::collections::BTreeMap;
use std::fmt::{Debug, Formatter};
use std::io;
use std::io::Write;
use std::net::Shutdown;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use mio::event::Event;
use mio::{Events, Interest, Poll, Token, Waker};
use slab::Slab;
use thiserror::Error;
use tracing::{debug, error, info, trace, warn};

use atlas_common::channel::{ChannelSyncRx, ChannelSyncTx, OneShotRx};
use atlas_common::error::*;
use atlas_common::node_id::{NodeId, NodeType};
use atlas_common::peer_addr::PeerAddr;
use atlas_common::socket::{MioListener, MioSocket, SecureSocket, SecureSocketSync, SyncListener};
use atlas_common::{channel, prng, quiet_unwrap, socket, Err};
use atlas_communication::byte_stub::{NodeIncomingStub, NodeStubController};
use atlas_communication::lookup_table::MessageModule;
use atlas_communication::message::{Header, NetworkSerializedMessage, WireMessage};
use atlas_communication::reconfiguration::{NetworkInformationProvider, NodeInfo};

use crate::conn_util;
use crate::conn_util::{
    interrupted, would_block, ConnCounts, ConnectionReadWork, ConnectionWriteWork, ReadingBuffer,
    WritingBuffer,
};
use crate::connections::{ByteMessageSendStub, Connections};

const DEFAULT_ALLOWED_CONCURRENT_JOINS: usize = 128;
// Since the tokens will always start at 0, we limit the amount of concurrent joins we can have
// And then make the server token that limit + 1, since we know that it will never be exceeded
// (Since slab re utilizes tokens)
const SERVER_TOKEN: Token = Token(DEFAULT_ALLOWED_CONCURRENT_JOINS + 1);

pub struct ConnectionHandler {
    my_id: NodeId,

    concurrent_conn: ConnCounts,
    currently_connecting: Mutex<BTreeMap<NodeId, usize>>,
}

/// A pending connection object, waiting for new information and to be accepted
/// By the connection handler
enum PendingConnection {
    PendingConn {
        peer_id: Option<NodeId>,
        node_type: Option<NodeType>,
        socket: MioSocket,
        read_buf: ReadingBuffer,
        write_buf: Option<WritingBuffer>,
        /// The channel that can be used to push requests to this connected node (to be sent to them)
        channel: Option<(
            ChannelSyncTx<NetworkSerializedMessage>,
            ChannelSyncRx<NetworkSerializedMessage>,
        )>,
    },
    Waker,
    ServerToken,
}

pub struct ServerWorker<NI, IS, CNP>
where
    NI: NetworkInformationProvider,
{
    my_id: NodeId,
    listener: MioListener,
    currently_accepting: Slab<PendingConnection>,
    conn_handler: Arc<ConnectionHandler>,
    network_info: Arc<NI>,
    peer_conns: Arc<Connections<NI, IS, CNP>>,
    waker: Arc<Waker>,
    poll: Poll,

    waker_token: Token,
    server_token: Token,
}

#[derive(Debug, Clone)]
enum ConnectionResult {
    Connected(NodeId, Vec<WireMessage>),
    Working,
    ConnectionBroken,
}

impl<NI, CN, CNP> ServerWorker<NI, CN, CNP>
where
    NI: NetworkInformationProvider + 'static,
    CN: NodeIncomingStub + 'static,
    CNP: NodeStubController<ByteMessageSendStub, CN> + 'static,
{
    pub fn new(
        my_id: NodeId,
        mut listener: MioListener,
        conn_handler: Arc<ConnectionHandler>,
        network_info: Arc<NI>,
        peer_conns: Arc<Connections<NI, CN, CNP>>,
    ) -> Result<Self> {
        let mut slab = Slab::with_capacity(DEFAULT_ALLOWED_CONCURRENT_JOINS);

        let poll = Poll::new()?;

        let (waker, waker_token) = {
            let entry = slab.vacant_entry();

            let waker_token = Token(entry.key());

            let waker = Waker::new(poll.registry(), waker_token)?;

            entry.insert(PendingConnection::Waker);

            (waker, waker_token)
        };

        let listener_token = {
            let entry = slab.vacant_entry();

            let listener_token = Token(entry.key());

            poll.registry()
                .register(&mut listener, listener_token, Interest::READABLE)?;

            entry.insert(PendingConnection::ServerToken);

            listener_token
        };

        Ok(Self {
            my_id,
            listener,
            currently_accepting: slab,
            conn_handler,
            network_info,
            peer_conns,
            waker: Arc::new(waker),
            poll,
            waker_token,
            server_token: listener_token,
        })
    }

    /// Run the event loop of this worker
    fn event_loop(mut self) -> Result<()> {
        let mut events = Events::with_capacity(DEFAULT_ALLOWED_CONCURRENT_JOINS);

        loop {
            self.poll
                .poll(&mut events, Some(Duration::from_millis(25)))?;

            for event in events.iter() {
                match event.token() {
                    token if token == self.server_token => {
                        self.accept_connections()
                            .context("Error while accepting connections")?;
                    }
                    token if token == self.waker_token => {
                        self.handle_write_request()
                            .context("Error while handling write requests")?;
                    }
                    token => {
                        let result = self
                            .handle_connection_ev(token, event)
                            .context("Error while handling connection event")?;

                        self.handle_connection_result(token, result)
                            .context("Error while handling connection results")?;
                    }
                }
            }
        }
    }

    /// Accept connections from the server listener
    fn accept_connections(&mut self) -> Result<()> {
        loop {
            match self.listener.accept() {
                Ok((socket, addr)) => {
                    trace!("{:?} // Received connection from {}", self.my_id, addr);

                    if self.currently_accepting.len() == DEFAULT_ALLOWED_CONCURRENT_JOINS {
                        // Ignore connections that would exceed our default concurrent join limit
                        warn!(" {:?} // Ignoring connection from {} since we have reached the concurrent join limit",
                            self.my_id, addr);

                        socket.shutdown(Shutdown::Both)?;

                        continue;
                    }

                    let mut read_buffer = BytesMut::with_capacity(Header::LENGTH);

                    read_buffer.resize(Header::LENGTH, 0);

                    let currently_accept = self
                        .currently_accepting
                        .insert(MioSocket::from(socket).into());

                    let token = Token(currently_accept);

                    let connection = &mut self.currently_accepting[token.into()];

                    match connection {
                        PendingConnection::PendingConn { socket, .. } => {
                            self.poll
                                .registry()
                                .register(socket, token, Interest::READABLE)
                                .context("Failed to register pending conn socket")?;
                        }
                        _ => unreachable!(),
                    }

                    let result = self
                        .handle_connection_readable(token)
                        .context("Error while handling readable connection")?;

                    debug!(
                        "{:?} // Connection from {} is {:?} (Token {:?})",
                        self.my_id, addr, result, token
                    );

                    self.handle_connection_result(token, result)
                        .context("Error while handling connection result")?;
                }
                Err(err) if would_block(&err) => {
                    // No more connections are ready to be accepted
                    break;
                }
                Err(ref err) if interrupted(err) => continue,
                Err(err) => {
                    return Err!(err);
                }
            }
        }

        Ok(())
    }

    fn handle_write_request(&mut self) -> io::Result<()> {
        let mut to_verify = Vec::with_capacity(self.currently_accepting.len());

        // This is a bit of a hack, but we need to do this in order to avoid issues with the borrow
        // Checker, since we would have to pass a mutable reference while holding immutable references.
        // It's stupid but it is what it is
        self.currently_accepting.iter().for_each(|(slot, conn)| {
            let token = Token(slot);

            if let PendingConnection::PendingConn { .. } = conn {
                to_verify.push(token);
            }
        });

        to_verify.into_iter().for_each(|token| {
            let connection_result = self.try_write_until_block(token).expect("Failed to write");

            if let ConnectionResult::Connected(_, _) = &connection_result {
                self.handle_connection_result(token, connection_result)
                    .expect("Failed to write");
            }
        });

        Ok(())
    }

    /// Handle the result of a pending connection having been reached.
    fn handle_connection_result(&mut self, token: Token, result: ConnectionResult) -> Result<()> {
        match result {
            ConnectionResult::Connected(node_id, mut pending_messages) => {
                let node = if let Some(first_msg) = pending_messages.pop() {
                    let node_info = first_msg.payload_buf();

                    let (node_info, _read): (NodeInfo, usize) = bincode::serde::decode_from_slice(
                        node_info.as_ref(),
                        bincode::config::standard(),
                    )?;

                    node_info
                } else {
                    error!(
                        "{:?} // Received connection from {:?} but no node info was received",
                        self.my_id, node_id
                    );

                    return Err(anyhow!(
                        "Received connection from {:?} but no node info was received",
                        node_id
                    ));
                };

                debug!(
                    "{:?} // Incoming connection to {:?} is now established with token {:?}, {:?}",
                    self.my_id,
                    node_id,
                    token,
                    self.currently_accepting
                        .iter()
                        .map(|(token, conn)| (Token(token), conn))
                        .collect::<Vec<_>>()
                );

                if let Some(connection) = self.currently_accepting.try_remove(token.into()) {
                    match connection {
                        PendingConnection::PendingConn {
                            mut socket,
                            channel,
                            write_buf,
                            read_buf,
                            ..
                        } => {
                            // Deregister from this poller as we are no longer
                            // the ones that should handle this connection
                            self.poll.registry().deregister(&mut socket)?;

                            let conn = self.peer_conns.handle_connection_established_with_socket(
                                node,
                                socket,
                                read_buf,
                                write_buf,
                                channel
                                    .unwrap_or_else(|| conn_util::initialize_send_channel(node_id)),
                            )?;

                            // We have identified the peer and should now handle the connection
                            for message in pending_messages {
                                if message.header().payload_length() > 0 {
                                    conn.byte_input_stub
                                        .handle_message(&self.network_info, message)?;
                                }
                            }
                        }
                        _ => unreachable!(),
                    }
                } else {
                    unreachable!()
                }
            }
            ConnectionResult::ConnectionBroken => {
                debug!(
                    "{:?} // Connection result as broken for token {:?}",
                    self.my_id, token
                );

                // Discard of the connection since it has been broken
                if let Some(connection) = self.currently_accepting.try_remove(token.into()) {
                    match connection {
                        PendingConnection::PendingConn { mut socket, .. } => {
                            self.poll.registry().deregister(&mut socket)?;
                        }
                        _ => unreachable!(),
                    }
                } else {
                    unreachable!()
                }
            }
            ConnectionResult::Working => {}
        }

        Ok(())
    }

    /// Handle connection events, received from epoll
    fn handle_connection_ev(&mut self, token: Token, ev: &Event) -> Result<ConnectionResult> {
        if ev.is_readable() {
            let connection_result = self.handle_connection_readable(token)?;

            match &connection_result {
                ConnectionResult::Connected(_, _) => {
                    return Ok(connection_result);
                }
                ConnectionResult::ConnectionBroken => {
                    return Ok(ConnectionResult::ConnectionBroken);
                }
                _ => {}
            }
        }

        if ev.is_writable() {
            let connection_result = self.try_write_until_block(token)?;

            match &connection_result {
                ConnectionResult::Connected(_, _) => {
                    return Ok(connection_result);
                }
                ConnectionResult::ConnectionBroken => {
                    return Ok(ConnectionResult::ConnectionBroken);
                }
                _ => {}
            }
        }

        Ok(ConnectionResult::Working)
    }

    fn try_write_until_block(&mut self, token: Token) -> io::Result<ConnectionResult> {
        let connection = &mut self.currently_accepting[token.into()];

        match connection {
            PendingConnection::PendingConn {
                socket,
                write_buf,
                channel,
                ..
            } => {
                let was_waiting_for_write = write_buf.is_some();
                let mut wrote = false;

                if let Some((_, rx)) = channel {
                    loop {
                        let writing = if let Some(writing_info) = write_buf {
                            wrote = true;

                            //We are writing something
                            writing_info
                        } else {
                            // We are not currently writing anything

                            match rx.try_recv() {
                                Ok(to_write) => {
                                    trace!("Writing message {:?}", to_write);
                                    wrote = true;

                                    // We have something to write
                                    *write_buf =
                                        Some(WritingBuffer::init_from_message(to_write).unwrap());

                                    write_buf.as_mut().unwrap()
                                }
                                Err(_) => {
                                    // Nothing to write
                                    trace!("Nothing left to write, wrote? {}", wrote);

                                    // If we have written something in this loop but we have not written until
                                    // Would block then we should flush the connection
                                    if wrote {
                                        match socket.flush() {
                                            Ok(_) => {}
                                            Err(ref err) if would_block(err) => break,
                                            Err(ref err) if interrupted(err) => continue,
                                            Err(err) => {
                                                return Err(err);
                                            }
                                        };
                                    }

                                    break;
                                }
                            }
                        };

                        match conn_util::try_write_until_block(socket, writing)
                            .expect("Failed to write to socket")
                        {
                            ConnectionWriteWork::ConnectionBroken => {
                                return Ok(ConnectionResult::ConnectionBroken);
                            }
                            ConnectionWriteWork::Working => {
                                break;
                            }
                            ConnectionWriteWork::Done => {
                                *write_buf = None;
                            }
                        }
                    }

                    if write_buf.is_none() && was_waiting_for_write {
                        // We have nothing more to write, so we no longer need to be notified of writability
                        self.poll
                            .registry()
                            .reregister(socket, token, Interest::READABLE)?;
                    } else if write_buf.is_some() && !was_waiting_for_write {
                        // We still have something to write but we reached a would block state,
                        // so we need to be notified of writability.
                        self.poll.registry().reregister(
                            socket,
                            token,
                            Interest::READABLE.add(Interest::WRITABLE),
                        )?;
                    } else {
                        // We have nothing to write and we were not waiting for writability, so we
                        // Don't need to re register
                        // Or we have something to write and we were already waiting for writability,
                        // So we also don't have to re register
                    }
                }
            }
            _ => unreachable!(),
        }

        Ok(ConnectionResult::Working)
    }

    fn handle_connection_readable(&mut self, token: Token) -> Result<ConnectionResult> {
        let connection = &mut self.currently_accepting[token.into()];
        trace!(
            "{:?} // Handling read event for connection {:?}",
            self.my_id,
            token
        );

        let result = match connection {
            PendingConnection::PendingConn {
                peer_id,
                socket,
                read_buf,
                node_type,
                ..
            } => {
                let read = conn_util::read_until_block(socket, read_buf)?;

                match read {
                    ConnectionReadWork::ConnectionBroken => ConnectionResult::ConnectionBroken,
                    ConnectionReadWork::Working => ConnectionResult::Working,
                    ConnectionReadWork::WorkingAndReceived(received)
                    | ConnectionReadWork::ReceivedAndDone(received) => {
                        let connection_peer_id = if let Some(message) = received.first() {
                            let header = message.header();

                            header.from()
                        } else {
                            trace!("Received empty message from {:?}", token);

                            return Ok(ConnectionResult::Working);
                        };

                        if peer_id.is_none() {
                            *peer_id = Some(connection_peer_id);

                            // Check the general connections first as we add to this before removing from the pending connections
                            match self.peer_conns.get_connection(&connection_peer_id) {
                                None => {
                                    debug!("Received connection ID for token {:?}, from {:?}. No existing connection has been found, initializing.", token, connection_peer_id,);

                                    return Ok(ConnectionResult::Connected(
                                        connection_peer_id,
                                        received,
                                    ));
                                }
                                Some(conn) => {
                                    trace!("Received connection ID for token {:?}, from {:?}, node type is: {:?}\
                                         (None means unknown) Connection already established", token, connection_peer_id, node_type);

                                    // This node is already known to us, we don't have to wait for reconfiguration messages
                                    let channel = conn.to_send.clone();

                                    connection.fill_channel(channel);

                                    return Ok(ConnectionResult::Connected(
                                        connection_peer_id,
                                        received,
                                    ));
                                }
                            }
                        }

                        for message in received {
                            self.peer_conns
                                .loopback()
                                .handle_message(&self.network_info, message)?;
                        }

                        ConnectionResult::Working
                    }
                }
            }
            _ => unreachable!(),
        };

        Ok(result)
    }
}

impl ConnectionHandler {
    pub(super) fn initialize(my_id: NodeId, conn_count: ConnCounts) -> Self {
        Self {
            my_id,
            concurrent_conn: conn_count,
            currently_connecting: Mutex::new(Default::default()),
        }
    }

    /// Register that we are currently attempting to connect to a node.
    /// Returns true if we can attempt to connect to this node, false otherwise
    /// We may not be able to connect to a given node if the amount of connections
    /// being established already overtakes the limit of concurrent connections
    fn register_connecting_to_node<NI>(&self, peer_id: NodeId, network_info: &NI) -> bool
    where
        NI: NetworkInformationProvider,
    {
        let mut connecting_guard = self.currently_connecting.lock().unwrap();

        let value = connecting_guard.entry(peer_id).or_insert(0);

        *value += 1;

        let other_node_type = network_info
            .get_node_info(&peer_id)
            .map(|info| info.node_type())
            .expect("Failed to get node type");

        if *value
            > self
                .concurrent_conn
                .get_connections_to_node(network_info.own_node_info().node_type(), other_node_type)
                * 2
        {
            *value -= 1;

            false
        } else {
            true
        }
    }

    /// Register that we are done connecting to a given node (The connection was either successful or failed)
    fn done_connecting_to_node(&self, peer_id: &NodeId) {
        let mut connection_guard = self.currently_connecting.lock().unwrap();

        connection_guard
            .entry(*peer_id)
            .and_modify(|value| *value -= 1);

        if let Some(connection_count) = connection_guard.get(peer_id) {
            if *connection_count == 0 {
                connection_guard.remove(peer_id);
            }
        }
    }

    pub fn connect_to_node<NI, CN, CNP>(
        self: &Arc<Self>,
        connections: Arc<Connections<NI, CN, CNP>>,
        peer_id: NodeId,
        addr: PeerAddr,
    ) -> std::result::Result<OneShotRx<Result<()>>, ConnectionEstablishError>
    where
        NI: NetworkInformationProvider + 'static,
        CN: NodeIncomingStub + 'static,
        CNP: NodeStubController<ByteMessageSendStub, CN> + 'static,
    {
        let (tx, rx) = channel::new_oneshot_channel();

        debug!(
            " {:?} // Connecting to node {:?} at {:?}",
            self.my_id(),
            peer_id,
            addr
        );

        let conn_handler = Arc::clone(self);

        if !self.register_connecting_to_node(peer_id, &*connections.network_info) {
            warn!(
                "{:?} // Tried to connect to node that I'm already connecting to {:?}",
                conn_handler.my_id(),
                peer_id
            );

            return Err!(ConnectionEstablishError::AlreadyConnectingToNode(peer_id));
        }

        let own_info = connections.network_info().own_node_info().clone();
        let other_node_info = connections
            .network_info()
            .get_node_info(&peer_id)
            .expect("Failed to get node info");

        std::thread::Builder::new()
            .name(format!("Connecting to Node {:?}", peer_id))
            .spawn(move || {

                //Get the correct IP for us to address the node
                //If I'm a client I will always use the client facing addr
                //While if I'm a replica I'll connect to the replica addr (clients only have this addr)
                let addr = addr.clone().into_inner();

                const SECS: u64 = 1;
                const RETRY: usize = 3 * 60;

                let mut rng = prng::State::new();

                let nonce = rng.next_state();

                let my_id = conn_handler.my_id();

                // NOTE:
                // ========
                //
                // 1) not an issue if `tx` is closed, this is not a
                // permanently running task, so channel send failures
                // are tolerated
                //
                // 2) try to connect up to `RETRY` times, then announce
                // failure
                for _try in 0..RETRY {
                    debug!("Attempting to connect to node {:?} with addr {:?} for the {} time", peer_id, addr, _try);

                    match socket::connect_sync(addr.0) {
                        Ok(mut sock) => {
                            let info = quiet_unwrap!(bincode::serde::encode_to_vec(&own_info, bincode::config::standard()));

                            // create header
                            let wm =
                                WireMessage::new(my_id, peer_id,
                                                 MessageModule::Reconfiguration,
                                                 Bytes::from(info), nonce,
                                                 None, None);

                            let write_info = WritingBuffer::init_from_message(wm).unwrap();

                            if let Err(err) = sock.write_all(write_info.current_header().as_ref().unwrap()) {
                                warn!("{:?} // Error while writing header on connecting to {:?} addr {:?}: {:?}",
                                    conn_handler.my_id(), peer_id, addr, err);

                                continue;
                            }

                            match sock.write(write_info.message_module().as_ref().unwrap()) {
                                Ok(size) => {
                                    trace!("{:?} // Wrote {:?} bytes for message module while initializing connection", conn_handler.my_id(), size);
                                }
                                Err(err) => {
                                    warn!("{:?} // Error while writing payload on connecting to {:?} addr {:?}: {:?}",
                                    conn_handler.my_id(), peer_id, addr, err);

                                    continue;
                                }
                            }

                            if let Err(err) = sock.write_all(write_info.current_message()) {
                                warn!("{:?} // Error while writing payload on connecting to {:?} addr {:?}: {:?}",
                                    conn_handler.my_id(), peer_id, addr, err);

                                continue;
                            }

                            if let Err(err) = sock.flush() {
                                warn!("{:?} // Error while flushing on connecting to {:?} addr {:?}: {:?}",
                                    conn_handler.my_id(), peer_id, addr, err);

                                continue;
                            }

                            // TLS handshake; drop connection if it fails
                            let sock = SecureSocketSync::new_plain(sock);

                            info!("{:?} // Established connection to node {:?}", my_id, peer_id);

                            let err = connections.handle_connection_established(other_node_info, SecureSocket::Sync(sock),
                                                                                ReadingBuffer::init_with_size(Header::LENGTH),
                                                                                None);

                            if err.is_err() {
                                let _ = tx.send(err);

                                return;
                            }

                            conn_handler.done_connecting_to_node(&peer_id);

                            let _ = tx.send(Ok(()));

                            return;
                        }
                        Err(err) => {
                            warn!("{:?} // Error on connecting to {:?} addr {:?}: {:?}",
                                conn_handler.my_id(), peer_id, addr, err);
                        }
                    }

                    // sleep for `SECS` seconds and retry
                    std::thread::sleep(Duration::from_secs(SECS));
                }

                conn_handler.done_connecting_to_node(&peer_id);

                // announce we have failed to connect to the peer node
                //if we fail to connect, then just ignore
                error!("{:?} // Failed to connect to the node {:?} ", conn_handler.my_id(), peer_id);

                let _ = tx.send(Err!(ConnectionEstablishError::FailedToConnectToNode(peer_id)));
            }).expect("Failed to allocate thread to establish connection");

        Ok(rx)
    }

    pub fn my_id(&self) -> NodeId {
        self.my_id
    }
}

pub fn initialize_server<NI, CN, CNP>(
    my_id: NodeId,
    listener: SyncListener,
    connection_handler: Arc<ConnectionHandler>,
    network_info: Arc<NI>,
    conns: Arc<Connections<NI, CN, CNP>>,
) -> Arc<Waker>
where
    NI: NetworkInformationProvider + 'static,
    CN: NodeIncomingStub + 'static,
    CNP: NodeStubController<ByteMessageSendStub, CN> + 'static,
{
    let server_worker = ServerWorker::new(
        my_id,
        listener.into(),
        connection_handler.clone(),
        network_info,
        conns,
    )
    .unwrap();

    let waker = server_worker.waker.clone();

    std::thread::Builder::new()
        .name(format!("Server Worker {:?}", my_id))
        .spawn(move || match server_worker.event_loop() {
            Ok(_) => {}
            Err(error) => {
                error!("Error in server worker {:?} {:?}", my_id, error)
            }
        })
        .expect("Failed to allocate thread for server worker");

    waker
}

impl PendingConnection {
    fn fill_channel(
        &mut self,
        ch: (
            ChannelSyncTx<NetworkSerializedMessage>,
            ChannelSyncRx<NetworkSerializedMessage>,
        ),
    ) {
        match self {
            PendingConnection::PendingConn { channel, .. } => {
                *channel = Some(ch);
            }
            _ => unreachable!(),
        }
    }
}

impl From<MioSocket> for PendingConnection {
    fn from(socket: MioSocket) -> Self {
        let read_buf = ReadingBuffer::init_with_size(Header::LENGTH);

        Self::PendingConn {
            peer_id: None,
            node_type: None,
            socket,
            read_buf,
            write_buf: None,
            channel: None,
        }
    }
}

impl Debug for PendingConnection {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            PendingConnection::PendingConn {
                peer_id,
                node_type,
                socket,
                ..
            } => {
                write!(
                    f,
                    "Peer conn {:?}, type {:?}, addr {:?}",
                    peer_id,
                    node_type,
                    socket.peer_addr()
                )
            }
            PendingConnection::Waker => {
                write!(f, "Waker")
            }
            PendingConnection::ServerToken => {
                write!(f, "ServerToken")
            }
        }
    }
}

#[derive(Error, Debug)]
pub enum ConnectionEstablishError {
    #[error("Failed to connect to node {0:?} as we are already connecting to that node")]
    AlreadyConnectingToNode(NodeId),
    #[error("Failed to connect to node {0:?}")]
    FailedToConnectToNode(NodeId),
}
