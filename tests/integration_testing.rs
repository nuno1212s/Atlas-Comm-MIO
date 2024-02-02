use std::collections::BTreeMap;
use std::fs::File;
use std::io::BufReader;
use std::iter;
use std::net::{SocketAddr, SocketAddrV4};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use anyhow::{anyhow, Context};
use getset::Getters;
use log::{debug, info};
use rustls::{ClientConfig, RootCertStore, ServerConfig};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls_pemfile::{Item, read_one};
use atlas_comm_mio::ByteStubType;
use atlas_comm_mio::config::{MIOConfig, TcpConfig, TlsConfig};
use atlas_common::channel;
use atlas_common::channel::{ChannelSyncRx, ChannelSyncTx};
use atlas_common::error::*;
use atlas_common::crypto::signature::{KeyPair, PublicKey};
use atlas_common::node_id::{NodeId, NodeType};
use atlas_common::peer_addr::PeerAddr;
use atlas_communication::byte_stub::{ByteNetworkStub, NodeIncomingStub, NodeStubController};
use atlas_communication::message::WireMessage;
use atlas_communication::reconfiguration::NetworkInformationProvider;

#[derive(Clone)]
struct MockStubController {
    stubs: Arc<Mutex<BTreeMap<NodeId, MockStub>>>,
    channel_tx: ChannelSyncTx<WireMessage>,
    channel_rx: ChannelSyncRx<WireMessage>,
}

#[derive(Clone)]
struct MockStubInput(NodeId, ChannelSyncTx<WireMessage>);

#[derive(Clone)]
struct MockStubOutput(NodeId, ByteStubType);

#[derive(Clone)]
struct MockStub(MockStubInput, MockStubOutput);

impl NodeIncomingStub for MockStubInput {
    fn handle_message<NI>(&self, network_info: &Arc<NI>, message: WireMessage) -> Result<()> where NI: NetworkInformationProvider + 'static {
        debug!("{:?} // Received message: {:?}", self.0, message.header());

        self.1.send(message)
    }
}


impl MockStubController {
    fn generate_stub_for(&self, node: NodeId, node_type: NodeType) -> MockStubInput {
        let tx = self.channel_tx.clone();
        let stub = MockStubInput(node, tx);
        stub
    }
}

impl NodeStubController<ByteStubType, MockStubInput> for MockStubController {
    fn has_stub_for(&self, node: &NodeId) -> bool {
        self.stubs.lock().unwrap().contains_key(node)
    }

    fn generate_stub_for(&self, node: NodeId, node_type: NodeType, byte_stub: ByteStubType) -> Result<MockStubInput> {
        let input_stub = self.generate_stub_for(node, node_type);

        let output_stub = MockStubOutput(node, byte_stub);

        let stub = MockStub(input_stub.clone(), output_stub);

        self.stubs.lock().unwrap().insert(node, stub);

        Ok(input_stub)
    }

    fn get_stub_for(&self, node: &NodeId) -> Option<MockStubInput> {
        self.stubs.lock().unwrap().get(node).map(|stub| stub.0.clone())
    }

    fn shutdown_stubs_for(&self, node: &NodeId) {
        self.stubs.lock().unwrap().remove(node);
    }
}

impl Default for MockStubController {
    fn default() -> Self {
        let (tx, rx) = channel::new_bounded_sync(32, Some("MockStubController"));
        Self {
            stubs: Arc::new(Mutex::new(Default::default())),
            channel_tx: tx,
            channel_rx: rx,
        }
    }
}

#[inline]
fn read_private_keys_from_file(mut file: BufReader<File>) -> Result<Vec<PrivateKeyDer<'static>>> {
    let mut certs = Vec::new();

    for item in iter::from_fn(|| read_one(&mut file).transpose()) {
        match item.context("Failed to read private key from file")? {
            Item::Pkcs1Key(rsa) => {
                certs.push(PrivateKeyDer::Pkcs1(rsa))
            }
            Item::Pkcs8Key(rsa) => {
                certs.push(PrivateKeyDer::Pkcs8(rsa))
            }
            Item::Sec1Key(rsa) => {
                certs.push(PrivateKeyDer::Sec1(rsa))
            }
            _ => {
                return Err(anyhow!("Certificate given in place of a key"));
            }
        }
    }

    Ok(certs)
}

fn read_certificates_from_file(mut file: &mut BufReader<File>) -> Result<Vec<CertificateDer<'static>>> {
    let mut certs = Vec::new();

    for item in iter::from_fn(|| read_one(&mut file).transpose()) {
        match item.context("Failed to read certificate from file")? {
            Item::X509Certificate(cert) => {
                certs.push(cert);
            }
            _ => {
                return Err(anyhow!("Key given in place of a certificate"));
            }
        }
    }

    Ok(certs)
}

fn default_config(node: u32) -> Result<MIOConfig> {
    info!("Loading configuration for node {}", node);
    info!("Current directory: {:?}", std::env::current_dir()?);

    let mut root_store = RootCertStore::empty();

    let cert = {
        let mut file = BufReader::new(File::open("./tests/ca-root/crt")
            .context("Failed to open certificate file")
            .context(format!("Current dir: {:?}", std::env::current_dir()))?);
        let certs = read_certificates_from_file(&mut file)?;

        root_store.add(certs[0].clone()).context("Failed to add certificate to root store")?;

        certs
    };

    let chain = {
        let mut file = BufReader::new(File::open(format!("./tests/ca-root/srv{}/chain", node))
            .context("Failed to open certificate file")
            .context(format!("Current dir: {:?}", std::env::current_dir()))?);
        let mut certs = read_certificates_from_file(&mut file)?;

        certs.extend(cert);

        certs
    };

    let sk = {
        let file = BufReader::new(File::open(format!("./tests/ca-root/srv{}/key", node))
            .context("Failed to open certificate file")
            .context(format!("Current dir: {:?}", std::env::current_dir()))?);

        read_private_keys_from_file(file)?.pop().ok_or(anyhow!("No private key found"))?
    };

    let client_config = ClientConfig::builder()
        .with_root_certificates(rustls::RootCertStore::empty()).with_no_client_auth();
    let server_config = ServerConfig::builder().with_no_client_auth().with_single_cert(chain, sk).unwrap();

    Ok(MIOConfig {
        epoll_worker_count: 2,
        tcp_configs: TcpConfig {
            network_config: TlsConfig {
                async_client_config: client_config.clone(),
                async_server_config: server_config.clone(),
                sync_server_config: server_config,
                sync_client_config: client_config,
            },
            replica_concurrent_connections: 1,
            client_concurrent_connections: 1,
        },
    })
}

#[derive(Getters, Clone)]
#[get = "pub(crate)"]
pub struct NodeInfo<K> where K: Clone {
    id: NodeId,
    addr: PeerAddr,
    node_type: NodeType,
    key: K,
}

pub struct MockNetworkInfo {
    own_node: NodeInfo<Arc<KeyPair>>,
    other_nodes: BTreeMap<NodeId, NodeInfo<PublicKey>>,
}

impl NetworkInformationProvider for MockNetworkInfo {
    fn get_own_id(&self) -> NodeId {
        self.own_node.id
    }

    fn get_own_addr(&self) -> PeerAddr {
        self.own_node.addr.clone()
    }

    fn get_key_pair(&self) -> &Arc<KeyPair> {
        &self.own_node.key
    }

    fn get_own_node_type(&self) -> NodeType {
        self.own_node.node_type
    }

    fn get_node_type(&self, node: &NodeId) -> Option<NodeType> {
        self.other_nodes.get(node).map(|info| info.node_type)
    }

    fn get_public_key(&self, node: &NodeId) -> Option<PublicKey> {
        self.other_nodes.get(node).map(|info| info.key.clone())
    }

    fn get_addr_for_node(&self, node: &NodeId) -> Option<PeerAddr> {
        self.other_nodes.get(node).map(|info| info.addr.clone())
    }
}

struct MockNetworkInfoFactory {
    nodes: BTreeMap<NodeId, NodeInfo<Arc<KeyPair>>>,
}

impl MockNetworkInfoFactory {
    const PORT: u32 = 10000;

    fn initialize_for(node_count: usize) -> Result<Self> {
        let buf = [0; 32];
        let mut map = BTreeMap::default();

        for node_id in 0..node_count {
            let key = KeyPair::from_bytes(buf.as_slice())?;

            let info = NodeInfo {
                id: NodeId::from(node_id as u32),
                addr: PeerAddr::new(format!("127.0.0.1:{}", Self::PORT + (node_id as u32)).parse()?, String::from("localhost")),
                node_type: NodeType::Replica,
                key: Arc::new(key),
            };

            map.insert(info.id.clone(), info);
        }

        Ok(Self {
            nodes: map
        })
    }

    fn generate_network_info_for(&self, node_id: NodeId) -> Result<MockNetworkInfo> {
        let own_network_id = self.nodes.get(&node_id)
            .ok_or(anyhow!("Node not found"))?
            .clone();

        let other_nodes: BTreeMap<NodeId, NodeInfo<PublicKey>> = self.nodes.iter().filter(|(id, _)| **id != node_id)
            .map(|(id, info)| {
                (id.clone(), NodeInfo {
                    id: info.id.clone(),
                    addr: info.addr.clone(),
                    node_type: info.node_type,
                    key: PublicKey::from(info.key.public_key()),
                })
            }).collect();

        Ok(MockNetworkInfo {
            own_node: own_network_id,
            other_nodes,
        })
    }
}

#[cfg(test)]
mod conn_test {
    use std::collections::BTreeMap;
    use std::mem::size_of;
    use std::sync::Arc;
    use anyhow::{anyhow, Context};
    use atlas_comm_mio::MIOTCPNode;
    use atlas_common::error::*;
    use atlas_common::node_id::NodeId;
    use atlas_communication::byte_stub::{ByteNetworkController, ByteNetworkControllerInit};
    use atlas_communication::byte_stub::connections::NetworkConnectionController;
    use atlas_communication::reconfiguration::ReconfigurationMessageHandler;
    use crate::{default_config, MockNetworkInfo, MockNetworkInfoFactory, MockStubController, MockStubInput};

    use std::sync::Once;
    use bytes::{Bytes, BytesMut};
    use log::{debug, info, warn};
    use atlas_communication::lookup_table::MessageModule;

    static INIT: Once = Once::new();

    /// Setup function that is only run once, even if called multiple times.
    fn setup() {
        INIT.call_once(|| {
            env_logger::builder().is_test(true).try_init().expect("Failed to initialize logger");
        });
    }

    fn initialize_node_set(node_count: u32) -> Result<BTreeMap<NodeId, MIOTCPNode<MockNetworkInfo, MockStubInput, MockStubController>>> {
        let factory = MockNetworkInfoFactory::initialize_for(node_count as usize)?;

        let mut nodes = BTreeMap::default();

        for node in 0..node_count {
            // Initialize all of the nodes
            let node = NodeId::from(node);

            let reconf = ReconfigurationMessageHandler::initialize();

            let network_info = factory.generate_network_info_for(node)?;

            let mock_stub_controller = MockStubController::default();

            let nt_node = MIOTCPNode::initialize_controller(reconf.clone(), Arc::new(network_info), default_config(node.0)?, mock_stub_controller)?;

            nodes.insert(node, nt_node);
        }


        Ok(nodes)
    }


    #[test]
    pub fn test_message_mod_serialization() -> Result<()> {

        let msg_mod = MessageModule::Protocol;

        let vec = bincode::serde::encode_to_vec(&msg_mod, bincode::config::standard())?;

        let mod_bytes = Bytes::from(vec);

        println!("Mod bytes {:x?}", mod_bytes);

        let (de_ser_msg_mod, msg_mod_size): (MessageModule, usize) = bincode::serde::decode_borrowed_from_slice(mod_bytes.as_ref(), bincode::config::standard())?;

        assert_eq!(de_ser_msg_mod, msg_mod);

        Ok(())
    }

    #[test]
    pub fn test_conn() -> Result<()> {
        setup();

        const NODE_COUNT: u32 = 3;

        debug!("Initializing node set");

        let nodes = initialize_node_set(NODE_COUNT)?;

        warn!("Node set initialized, making connections");

        for node in 0..NODE_COUNT {
            let node_id = NodeId::from(node);

            info!("Connecting node {:?}", node_id);

            let node_ref = nodes.get(&node_id).ok_or(anyhow!("Node not found"))?;

            // Since a <-> b => b <-> a, then 1 only needs to connect to 2, and 2 only needs to connect to 3
            for other_node in node + 1..NODE_COUNT {
                let other_node_id = NodeId::from(other_node);

                info!("Connecting to node {:?}", other_node_id);

                for result_wait in node_ref.connection_controller().connect_to_node(other_node_id) {
                    result_wait.recv().unwrap()
                        .context(format!("Failed to receive result of connecting to node {:?}", other_node_id))?;
                };


            }
        }

        for node in 0..NODE_COUNT {
            let node_ref = nodes.get(&NodeId::from(node)).unwrap();

            for other_node in 0..NODE_COUNT {
                if other_node == node {
                    continue;
                }

                assert!(node_ref.connection_controller().has_connection(&NodeId::from(other_node)),
                "Node {:?} does not have connection to node {:?}", node, other_node);
            }
        }

        Ok(())
    }
}