#![allow(dead_code, clippy::large_enum_variant)]

use crate::conn_util;
use crate::conn_util::{
    interrupted, would_block, ConnectionReadWork, ConnectionWriteWork, ReadingBuffer, WritingBuffer,
};
use crate::connections::{ByteMessageSendStub, ConnHandle, Connections, PeerConn};
use crate::epoll::{EpollWorkerId, EpollWorkerMessage, NewConnection};
use anyhow::Context;
use atlas_common::channel::ChannelSyncRx;
use atlas_common::node_id::NodeId;
use atlas_common::socket::MioSocket;
use atlas_common::Err;
use atlas_communication::byte_stub::{NodeIncomingStub, NodeStubController};

use atlas_communication::reconfiguration::NetworkInformationProvider;
use mio::event::Event;
use mio::{Events, Interest, Poll, Token, Waker};
use slab::Slab;
use std::io;
use std::io::Write;
use std::net::Shutdown;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, trace};

const EVENT_CAPACITY: usize = 1024;
const DEFAULT_SOCKET_CAPACITY: usize = 1024;
const WORKER_TIMEOUT: Option<Duration> = Some(Duration::from_millis(50));

enum ConnectionWorkResult {
    Working,
    ConnectionBroken,
}

type ConnectionRegister = ChannelSyncRx<MioSocket>;

pub(crate) struct EpollWorker<NI, CN, CNP>
where
    NI: NetworkInformationProvider,
{
    worker_id: EpollWorkerId,

    global_conns: Arc<Connections<NI, CN, CNP>>,

    connections: Slab<SocketConnection<CN>>,

    conn_register: ChannelSyncRx<EpollWorkerMessage<CN>>,

    // MIO related stuff
    poll: Poll,
    waker: Arc<Waker>,
    waker_token: Token,
}

enum SocketConnection<CN> {
    PeerConn {
        handle: ConnHandle,
        socket: MioSocket,
        reading_info: ReadingBuffer,
        writing_info: Option<WritingBuffer>,
        connection: Arc<PeerConn<CN>>,
    },
    Waker,
}

impl<CN> SocketConnection<CN> {
    pub(crate) fn peer_id(&self) -> Option<NodeId> {
        match self {
            SocketConnection::PeerConn { handle, .. } => Some(handle.peer_id()),
            SocketConnection::Waker => None,
        }
    }
}

impl<NI, CN, CNP> EpollWorker<NI, CN, CNP>
where
    CN: NodeIncomingStub + 'static,
    NI: NetworkInformationProvider + 'static,
    CNP: NodeStubController<ByteMessageSendStub, CN> + 'static,
{
    /// Initializing a worker thread for the worker group
    pub(crate) fn new(
        worker_id: EpollWorkerId,
        connections: Arc<Connections<NI, CN, CNP>>,
        register: ChannelSyncRx<EpollWorkerMessage<CN>>,
    ) -> atlas_common::error::Result<Self> {
        let poll = Poll::new().context(format!(
            "Failed to initialize poll for worker {:?}",
            worker_id
        ))?;

        let mut conn_slab = Slab::with_capacity(DEFAULT_SOCKET_CAPACITY);

        let entry = conn_slab.vacant_entry();

        let waker_token = Token(entry.key());
        let waker = Arc::new(
            Waker::new(poll.registry(), waker_token)
                .context(format!("Failed to create waker for worker {:?}", worker_id))?,
        );

        entry.insert(SocketConnection::Waker);

        info!(
            "{:?} // Initialized Epoll Worker where Waker is token {:?}",
            connections.own_id(),
            waker_token
        );

        Ok(Self {
            worker_id,
            global_conns: connections,
            connections: conn_slab,
            conn_register: register,
            poll,
            waker,
            waker_token,
        })
    }

    pub(super) fn epoll_worker_loop(mut self) -> atlas_common::error::Result<()> {
        let mut event_queue = Events::with_capacity(EVENT_CAPACITY);

        let my_id = self.global_conns.own_id();

        let waker_token = self.waker_token;

        loop {
            if let Err(e) = self.poll.poll(&mut event_queue, WORKER_TIMEOUT) {
                if e.kind() == io::ErrorKind::Interrupted {
                    // spurious wakeup
                    continue;
                } else if e.kind() == io::ErrorKind::TimedOut {
                    // *should* be handled by mio and return Ok() with no events
                    continue;
                } else {
                    return Err!(e);
                }
            }

            trace!(
                "{:?} // Worker {}: Handling {} events {:?}",
                self.global_conns.own_id(),
                self.worker_id,
                event_queue.iter().count(),
                event_queue
            );

            for event in event_queue.iter() {
                if event.token() == waker_token {
                    // Indicates that we should try to write from the connections

                    // This is a bit of a hack, but we need to do this in order to avoid issues with the borrow
                    // Checker, since we would have to pass a mutable reference while holding immutable references.
                    // It's stupid but it is what it is
                    let mut to_verify = Vec::with_capacity(self.connections.len());

                    self.connections.iter().for_each(|(slot, conn)| {
                        let token = Token(slot);

                        if let SocketConnection::PeerConn { .. } = conn {
                            to_verify.push(token);
                        }
                    });

                    to_verify.into_iter().for_each(|token| {
                        match self.try_write_until_block(token) {
                            Ok(ConnectionWorkResult::ConnectionBroken) => {
                                let peer_id = {
                                    let connection = &self.connections[token.into()];

                                    connection.peer_id().unwrap_or(NodeId::from(1234567u32))
                                };

                                error!("{:?} // Connection broken during reading. Deleting connection {:?} to node {:?}",
                                    my_id, token,peer_id);

                                if let Err(err) = self.delete_connection(token, true) {
                                    error!("{:?} // Error deleting connection {:?} to node {:?}: {:?}",
                                        my_id, token, peer_id, err);
                                }
                            }
                            Ok(_) => {}
                            Err(err) => {
                                let peer_id = {
                                    let connection = &self.connections[token.into()];

                                    connection.peer_id().unwrap_or(NodeId::from(1234567u32))
                                };

                                error!("{:?} // Error handling connection event: {:?} for token {:?} (corresponding to conn id {:?})",
                                            my_id, err, token, peer_id);

                                if let Err(err) = self.delete_connection(token, true) {
                                    error!("{:?} // Error deleting connection {:?} to node {:?}: {:?}",
                                        my_id, token, peer_id, err);
                                }
                            }
                        };
                    });
                } else {
                    let token = event.token();

                    if !self.connections.contains(token.into()) {
                        // In case the waker already deleted this socket
                        continue;
                    }

                    match self.handle_connection_event(token, event) {
                        Ok(ConnectionWorkResult::ConnectionBroken) => {
                            let peer_id = {
                                let connection = &self.connections[token.into()];

                                connection.peer_id().unwrap_or(NodeId::from(1234567u32))
                            };

                            error!("{:?} // Connection broken during reading. Deleting connection {:?} to node {:?}",
                                    self.global_conns.own_id(), token,peer_id);

                            if let Err(err) = self.delete_connection(token, true) {
                                error!(
                                    "{:?} // Error deleting connection {:?} to node {:?}: {:?}",
                                    my_id, token, peer_id, err
                                );
                            }
                        }
                        Ok(_) => {}
                        Err(err) => {
                            let peer_id = {
                                let connection = &self.connections[token.into()];

                                connection.peer_id().unwrap_or(NodeId::from(1234567u32))
                            };

                            error!("{:?} // Error handling connection event: {:?} for token {:?} (corresponding to conn id {:?})",
                                            self.global_conns.own_id(), err, token, peer_id);

                            if let Err(err) = self.delete_connection(token, true) {
                                error!(
                                    "{:?} // Error deleting connection {:?} to node {:?}: {:?}",
                                    my_id, token, peer_id, err
                                );
                            }
                        }
                    }
                }
            }

            self.register_connections()?;
        }
    }

    fn handle_connection_event(
        &mut self,
        token: Token,
        event: &Event,
    ) -> atlas_common::error::Result<ConnectionWorkResult> {
        let _connection = if self.connections.contains(token.into()) {
            &self.connections[token.into()]
        } else {
            error!(
                "{:?} // Received event for non-existent connection with token {:?}",
                self.global_conns.own_id(),
                token
            );

            return Ok(ConnectionWorkResult::ConnectionBroken);
        };

        match &self.connections[token.into()] {
            SocketConnection::PeerConn { .. } => {
                if event.is_readable() {
                    if let ConnectionWorkResult::ConnectionBroken = self.read_until_block(token)? {
                        return Ok(ConnectionWorkResult::ConnectionBroken);
                    }
                }

                if event.is_writable() {
                    if let ConnectionWorkResult::ConnectionBroken =
                        self.try_write_until_block(token)?
                    {
                        return Ok(ConnectionWorkResult::ConnectionBroken);
                    }
                }
            }
            SocketConnection::Waker => {}
        }

        Ok(ConnectionWorkResult::Working)
    }

    fn try_write_until_block(
        &mut self,
        token: Token,
    ) -> atlas_common::error::Result<ConnectionWorkResult> {
        let connection = if self.connections.contains(token.into()) {
            &mut self.connections[token.into()]
        } else {
            error!(
                "{:?} // Received write event for non-existent connection with token {:?}",
                self.global_conns.own_id(),
                token
            );

            return Ok(ConnectionWorkResult::ConnectionBroken);
        };

        match connection {
            SocketConnection::PeerConn {
                socket,
                connection,
                writing_info,
                ..
            } => {
                let was_waiting_for_write = writing_info.is_some();
                let mut wrote = false;

                loop {
                    let writing = if let Some(writing_info) = writing_info {
                        wrote = true;

                        //We are writing something
                        writing_info
                    } else {
                        // We are not currently writing anything

                        if let Some(to_write) = connection.try_take_from_send()? {
                            trace!(
                                "{:?} // Writing message {:?}",
                                self.global_conns.own_id(),
                                to_write
                            );
                            wrote = true;

                            // We have something to write
                            *writing_info = Some(WritingBuffer::init_from_message(to_write)?);

                            writing_info.as_mut().unwrap()
                        } else {
                            // Nothing to write
                            trace!(
                                "{:?} // Nothing left to write, wrote? {}",
                                self.global_conns.own_id(),
                                wrote
                            );

                            // If we have written something in this loop but we have not written until
                            // Would block then we should flush the connection
                            if wrote {
                                match socket.flush() {
                                    Ok(_) => {}
                                    Err(ref err) if would_block(err) => break,
                                    Err(ref err) if interrupted(err) => continue,
                                    Err(err) => {
                                        return Err!(err);
                                    }
                                };
                            }

                            break;
                        }
                    };

                    match conn_util::try_write_until_block(socket, writing)? {
                        ConnectionWriteWork::ConnectionBroken => {
                            return Ok(ConnectionWorkResult::ConnectionBroken);
                        }
                        ConnectionWriteWork::Working => {
                            break;
                        }
                        ConnectionWriteWork::Done => {
                            *writing_info = None;
                        }
                    }
                }

                if writing_info.is_none() && was_waiting_for_write {
                    // We have nothing more to write, so we no longer need to be notified of writability
                    self.poll
                        .registry()
                        .reregister(socket, token, Interest::READABLE)
                        .context("Failed to reregister socket")?;
                } else if writing_info.is_some() && !was_waiting_for_write {
                    // We still have something to write but we reached a would block state,
                    // so we need to be notified of writability.
                    self.poll
                        .registry()
                        .reregister(socket, token, Interest::READABLE.add(Interest::WRITABLE))
                        .context("Failed to reregister socket")?;
                } else {
                    // We have nothing to write and we were not waiting for writability, so we
                    // Don't need to re register
                    // Or we have something to write and we were already waiting for writability,
                    // So we also don't have to re register
                }
            }
            _ => unreachable!(),
        }

        Ok(ConnectionWorkResult::Working)
    }

    fn read_until_block(
        &mut self,
        token: Token,
    ) -> atlas_common::error::Result<ConnectionWorkResult> {
        let connection = if self.connections.contains(token.into()) {
            &mut self.connections[token.into()]
        } else {
            error!(
                "{:?} // Received read event for non-existent connection with token {:?}",
                self.global_conns.own_id(),
                token
            );

            return Ok(ConnectionWorkResult::ConnectionBroken);
        };

        match connection {
            SocketConnection::PeerConn {
                socket,
                reading_info,
                connection,
                ..
            } => {
                match conn_util::read_until_block(socket, reading_info)? {
                    ConnectionReadWork::ConnectionBroken => {
                        return Ok(ConnectionWorkResult::ConnectionBroken);
                    }
                    ConnectionReadWork::Working => {
                        return Ok(ConnectionWorkResult::Working);
                    }
                    ConnectionReadWork::WorkingAndReceived(received)
                    | ConnectionReadWork::ReceivedAndDone(received) => {
                        // Handle the messages that we have received
                        // In this case by propagating them upwards in the architecture
                        for message in received {
                            connection
                                .byte_input_stub()
                                .handle_message(self.global_conns.network_info(), message)?;
                        }
                    }
                }
                // We don't have any more
            }
            _ => unreachable!(),
        }

        Ok(ConnectionWorkResult::Working)
    }

    /// Receive connections from the connection register and register them with the epoll instance
    fn register_connections(&mut self) -> atlas_common::error::Result<()> {
        while let Ok(message) = self.conn_register.try_recv() {
            match message {
                EpollWorkerMessage::NewConnection(conn) => {
                    self.create_connection(conn)?;
                }
                EpollWorkerMessage::CloseConnection(token) => {
                    if let SocketConnection::Waker = &self.connections[token.into()] {
                        // We can't close the waker, wdym?
                        continue;
                    }

                    self.delete_connection(token, false)?;
                }
            }
        }

        Ok(())
    }

    fn create_connection(&mut self, conn: NewConnection<CN>) -> atlas_common::error::Result<()> {
        let NewConnection {
            conn_id,
            peer_id,
            my_id,
            mut socket,
            reading_info,
            writing_info,
            peer_conn,
        } = conn;

        let entry = self.connections.vacant_entry();

        let token = Token(entry.key());

        let handle = ConnHandle::new(
            conn_id,
            my_id,
            peer_id,
            self.worker_id,
            token,
            self.waker.clone(),
        );

        peer_conn.register_peer_conn(handle.clone());

        self.poll
            .registry()
            .register(&mut socket, token, Interest::READABLE)?;

        let socket_conn = SocketConnection::PeerConn {
            handle: handle.clone(),
            socket,
            reading_info,
            writing_info,
            connection: peer_conn,
        };

        entry.insert(socket_conn);

        debug!(
            "{:?} // Registered new connection {:?} to node {:?} in epoll worker {:?}",
            self.global_conns.own_id(),
            token,
            peer_id,
            self.worker_id
        );

        self.read_until_block(token)?;
        self.try_write_until_block(token)?;

        Ok(())
    }

    fn delete_connection(
        &mut self,
        token: Token,
        is_failure: bool,
    ) -> atlas_common::error::Result<()> {
        if let Some(conn) = self.connections.try_remove(token.into()) {
            match conn {
                SocketConnection::PeerConn {
                    mut socket,
                    connection,
                    handle,
                    ..
                } => {
                    self.poll.registry().deregister(&mut socket)?;

                    if is_failure {
                        self.global_conns
                            .handle_connection_failed(handle.peer_id(), handle.id());
                    } else {
                        connection.delete_connection(handle.id());
                    }

                    socket.shutdown(Shutdown::Both)?;
                }
                _ => unreachable!("Only peer connections can be removed from the connection slab"),
            }
        } else {
            error!(
                "{:?} // Tried to remove a connection that doesn't exist, {:?}",
                self.global_conns.own_id(),
                token
            );
        }

        Ok(())
    }

    pub fn waker(&self) -> &Arc<Waker> {
        &self.waker
    }
}
