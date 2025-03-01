use futures::future::{join_all, JoinAll};
use hashbrown::hash_map::Entry;
use hashbrown::{HashMap, HashSet};
use itertools::Itertools;
use log::{debug, error, info, trace, warn};
use lru_time_cache::LruCache;
use tokio::net::UdpSocket;
use tokio::net::{lookup_host, ToSocketAddrs};
use tokio::spawn;
use tokio::sync::mpsc::Sender;
use tokio::sync::{mpsc, oneshot, Mutex, RwLock};
use tokio::task::JoinHandle;
use tokio::time::{interval, sleep};
use tokio_stream::wrappers::ReceiverStream;

use std::array::TryFromSliceError;
use std::collections::BinaryHeap;
use std::convert::TryInto;
use std::error::Error;
use std::fmt::Debug;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use crate::dht::krpc::{
    DhtNodeCompactListOwned, DhtQuery, DhtResponse, GetPeersResult, KRpc, KRpcBody,
};

pub mod krpc;
mod node;

pub use node::*;

pub trait RouteTable {
    fn id(&self) -> Arc<DhtNodeId>;
    fn update(&mut self, node: DhtNode);
    fn nodes_count(&self) -> usize;
    fn nearests(&self, id: &DhtNodeId) -> Vec<&SharedDhtNode>;
    fn unheathy(&self) -> Vec<SharedDhtNode>;
    fn clean_unheathy(&mut self) -> Vec<SharedDhtNode>;
    fn pick_node(&self) -> Option<&SharedDhtNode>;
}

pub struct DhtClient<R: RouteTable> {
    udp: Arc<UdpSocket>,
    trackers: Vec<SocketAddr>,
    router: Arc<RwLock<R>>,
    next_transaction_id: u16,
    id: Arc<DhtNodeId>,
    peer_lookup_tasks: Arc<Mutex<HashMap<InfoHash, PeerLookupTask>>>,
    get_peers_waiters: Arc<Mutex<LruCache<GetPeersRequestIndex, InfoHash>>>,
}

pub enum InfoHashObservation {
    WithPeerAddr {
        info_hash: Arc<InfoHash>,
        peer_addr: SocketAddr,
    },
    WithUtpPeerAddr {
        info_hash: Arc<InfoHash>,
        utp_addr: SocketAddr,
    },
    WithDhtNode {
        info_hash: Arc<InfoHash>,
        node: DhtNode,
    },
}

#[derive(Debug)]
struct PeerLookupTask {
    info_hash: Arc<InfoHash>,
    result: oneshot::Sender<Vec<SocketAddr>>,
    cloest_nodes: BinaryHeap<OrderedNode>,
    start_time: Instant,
    visited: HashSet<SocketAddr>,
}

#[derive(Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Clone)]
struct GetPeersRequestIndex {
    node: DhtNode,
    tid: u16,
}

impl GetPeersRequestIndex {
    pub fn new(
        id: &DhtNodeId,
        addr: SocketAddr,
        transaction_id: &[u8],
    ) -> Result<GetPeersRequestIndex, TryFromSliceError> {
        Ok(GetPeersRequestIndex {
            node: DhtNode {
                id: id.clone(),
                addr,
            },
            tid: u16::from_be_bytes(transaction_id.try_into()?),
        })
    }
    pub fn new_with_node(
        node: &DhtNode,
        transaction_id: &[u8],
    ) -> Result<GetPeersRequestIndex, TryFromSliceError> {
        Ok(GetPeersRequestIndex {
            node: node.clone(),
            tid: u16::from_be_bytes(transaction_id.try_into()?),
        })
    }
}

pub struct PeerLookupSender(Sender<PeerLookupTask>, Arc<DhtNodeId>);

impl PeerLookupSender {
    pub async fn lookup_peers(
        &self,
        info_hash: &InfoHash,
        init_node: Option<&DhtNode>,
    ) -> Result<Vec<SocketAddr>, Box<dyn Error>> {
        let (tx, rx) = oneshot::channel();
        self.0
            .send_timeout(
                PeerLookupTask {
                    info_hash: Arc::new(info_hash.clone()),
                    result: tx,
                    cloest_nodes: init_node
                        .map(|node| node.distence_order(&self.1))
                        .into_iter()
                        .collect(),
                    start_time: Instant::now(),
                    visited: Default::default(),
                },
                Duration::from_millis(500),
            )
            .await?;

        Ok(rx.await?)
    }
}

pub struct DhtClientController {
    main: JoinAll<tokio::task::JoinHandle<()>>,
    info_hash_observer: ReceiverStream<InfoHashObservation>,
    peer_lockup: Sender<PeerLookupTask>,
    id: Arc<DhtNodeId>,
}

// impl Drop for DhtClientController {
//     fn drop(&mut self) {
//         spawn(self.main).abort()
//     }
// }

impl DhtClientController {
    pub fn stop(self) {
        spawn(self.main).abort()
    }

    /// Get a mutable reference to the dht client controller's info hash observer.
    pub fn info_hash_observer(&mut self) -> &mut ReceiverStream<InfoHashObservation> {
        &mut self.info_hash_observer
    }

    /// Get a clone to the dht client controller's id.
    pub fn id(&self) -> Arc<DhtNodeId> {
        self.id.clone()
    }

    /// Get a clone to the dht client controller's peer lockup.
    pub fn sender(&self) -> Arc<Mutex<PeerLookupSender>> {
        Arc::new(Mutex::new(PeerLookupSender(
            self.peer_lockup.clone(),
            self.id.clone(),
        )))
    }
}

impl<R: RouteTable + Default> DhtClient<R> {
    pub async fn new() -> Result<Self, std::io::Error> {
        let socket: UdpSocket = UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 5717)).await?;
        let router: Arc<RwLock<R>> = Default::default();
        let id = router.read().await.id().clone();
        Ok(DhtClient {
            udp: socket.into(),
            trackers: vec![],
            router,
            next_transaction_id: Default::default(),
            id,
            peer_lookup_tasks: Default::default(),
            get_peers_waiters: Arc::new(Mutex::new(LruCache::with_expiry_duration(
                Duration::from_secs(5),
            ))),
        })
    }
}

impl<R> DhtClient<R>
where
    R: RouteTable + Send + Sync,
    R: 'static,
{
    pub async fn add_trackers<T>(&mut self, trackers: &[T]) -> usize
    where
        T: ToSocketAddrs + Clone,
    {
        let old_size = self.trackers.len();
        let local = self.udp.local_addr().unwrap();
        for tracker in trackers {
            if let Ok(addrs) = lookup_host(tracker).await {
                self.trackers
                    .extend(addrs.filter(|addr| addr.is_ipv4() == local.is_ipv4()));
            }
        }
        self.trackers.len() - old_size
    }

    pub fn run(mut self) -> DhtClientController {
        let (find_info_hash, info_hash_observer) = mpsc::channel::<InfoHashObservation>(32);
        let (peer_lockup, peer_lockup_rx) = mpsc::channel::<PeerLookupTask>(32);
        let (find_node, rx) = mpsc::channel::<DhtNode>(128);

        let route_maintaining = Self::start_route_maintaining_thread(
            self.router.clone(),
            self.trackers.clone(),
            find_node.clone(),
        );
        let myid = self.id.clone();
        let peer_lookup_task = Self::start_lookup_peer_thread(
            myid.clone(),
            self.router.clone(),
            self.udp.clone(),
            self.peer_lookup_tasks.clone(),
            self.get_peers_waiters.clone(),
            peer_lockup_rx,
        );
        let find_node_sender = Self::start_find_node_thread(myid.clone(), self.udp.clone(), rx);
        let crawler = Self::start_crawler_thread(self.router.clone(), find_node.clone());

        let main = spawn(async move {
            let mut buf = vec![0_u8; u16::MAX as usize];
            loop {
                match self.udp.recv_from(&mut *buf).await {
                    Ok((len, addr)) => {
                        debug!("{:?} bytes received from {:?}", len, addr);

                        if let Ok(packet) = krpc::from_bytes(&buf[0..len]) {
                            if let Some(id) = packet.node_id() {
                                let node = DhtNode::new(id.clone(), addr.clone());
                                trace!("Updating Node {}", &node);
                                self.router.write().await.update(node);
                            }
                            self.process_packet(packet, addr, &find_node, &find_info_hash)
                                .await;
                        } else {
                            warn!("Unknown packet received from {:?}", addr);
                        }
                    }
                    Err(err) => {
                        // Ignore UDP `WSAECONNRESET`
                        if err.raw_os_error() != Some(10054) {
                            error!("Failed to read packet {}", err)
                        }
                    }
                }
            }
        });
        return DhtClientController {
            main: join_all([
                main,
                route_maintaining,
                find_node_sender,
                crawler,
                peer_lookup_task,
            ]),
            info_hash_observer: ReceiverStream::new(info_hash_observer),
            peer_lockup,
            id: myid,
        };
    }

    async fn process_packet(
        &mut self,
        packet: KRpc<'_>,
        addr: SocketAddr,
        find_node: &mpsc::Sender<DhtNode>,
        find_info_hash: &Sender<InfoHashObservation>,
    ) {
        match packet {
            KRpc {
                transaction_id,
                body: KRpcBody::Query(query),
                ..
            } => {
                if let Err(err) = match query {
                    DhtQuery::AnnouncePeer {
                        info_hash,
                        implied_port,
                        port,
                        ..
                    } => {
                        let new_info_hash = Arc::new(info_hash.clone());
                        let observation = if implied_port.map(|i| i > 0).unwrap_or_default() {
                            InfoHashObservation::WithUtpPeerAddr {
                                info_hash: new_info_hash,
                                utp_addr: addr.clone(),
                            }
                        } else {
                            InfoHashObservation::WithPeerAddr {
                                info_hash: new_info_hash,
                                peer_addr: {
                                    let mut peer_addr = addr.clone();
                                    peer_addr.set_port(port);
                                    peer_addr
                                },
                            }
                        };
                        let _ = find_info_hash.try_send(observation);
                        trace!("Got seed hash (AnnouncePeer): {} from {}", info_hash, addr);
                        self.reply_ping(transaction_id, &addr).await
                    }
                    DhtQuery::FindNode { target, .. } => {
                        self.reply_find_node(transaction_id, target, &addr).await
                    }
                    DhtQuery::GetPeers { info_hash, id } => {
                        let observation = InfoHashObservation::WithDhtNode {
                            info_hash: Arc::new(info_hash.clone()),
                            node: DhtNode::new(id.clone(), addr.clone()),
                        };
                        let _ = find_info_hash.try_send(observation);
                        trace!("Got seed hash (GetPeers): {} from {}", info_hash, addr);
                        self.reply_get_peers(transaction_id, info_hash, &addr).await
                    }
                    DhtQuery::Ping { .. } => self.reply_ping(transaction_id, &addr).await,
                } {
                    error!("Failed to send reply: {}", err)
                }
            }
            KRpc {
                body: KRpcBody::Response { response },
                transaction_id,
                ..
            } => match response {
                DhtResponse::FindNode { nodes, .. } => {
                    for new_node in nodes.nodes().unique() {
                        let _ = find_node.try_send(new_node.into());
                    }
                }
                DhtResponse::GetPeers { id, result, .. } => {
                    self.handle_get_peers_response(id, addr, transaction_id, result, find_node)
                        .await
                }
                _ => (),
            },
            KRpc {
                body: KRpcBody::Error { error },
                ..
            } => {
                warn!("Received error package <{:?}> from {}", error, addr)
            }
        };
    }

    async fn handle_get_peers_response(
        &mut self,
        id: &DhtNodeId,
        addr: SocketAddr,
        transaction_id: &[u8],
        result: GetPeersResult<'_>,
        find_node: &Sender<DhtNode>,
    ) {
        let dht_node = match GetPeersRequestIndex::new(id, addr, transaction_id) {
            Ok(dht_node) => dht_node,
            _ => return,
        };
        let info_hash = { self.get_peers_waiters.lock().await.remove(&dht_node) };
        if let Some(info_hash) = info_hash {
            match result {
                GetPeersResult::Found(peers) => {
                    if verify_peers(&peers) {
                        trace!(
                            "Invalid get_peers value from {} for hash {}",
                            addr,
                            info_hash
                        );
                        return;
                    }
                    if let Some(task) = self.peer_lookup_tasks.lock().await.remove(&info_hash) {
                        info!("Get peers Found {:?} from {}", peers, addr);
                        let result = peers.into_iter().map(|p| p.addr()).collect();
                        if task.result.send(result).is_err() {
                            warn!("Peers for {} already found, ignore", info_hash);
                        }
                    }
                }
                GetPeersResult::NotFound(nodes) => {
                    let mut peer_lookup_tasks = self.peer_lookup_tasks.lock().await;
                    if let Some(task) = peer_lookup_tasks.get_mut(&info_hash) {
                        // info!("Get peers NotFound {:?} from {}", nodes, addr);
                        let info_hash = task.info_hash.clone();
                        let id = self.id.clone();
                        let socket = self.udp.clone();
                        let get_peers_waiters = self.get_peers_waiters.clone();
                        let visited = &mut task.visited;
                        task.cloest_nodes.extend(
                            nodes
                                .nodes()
                                .unique()
                                .filter(|node| visited.insert(node.addr()))
                                .map(|node| DhtNode::from(node).distence_order(&info_hash)),
                        );
                        // Remove inactive task
                        if task.cloest_nodes.is_empty()
                            && self
                                .get_peers_waiters
                                .lock()
                                .await
                                .iter()
                                .find(|(_, hash)| *hash == info_hash.as_ref())
                                .is_none()
                        {
                            peer_lookup_tasks.remove(info_hash.as_ref());
                            return;
                        }
                        let cloest_nodes = pop_n(task);
                        spawn(async move {
                            let mut interval = interval(Duration::from_millis(10));
                            for node in cloest_nodes {
                                Self::send_get_peer(
                                    &id,
                                    &info_hash,
                                    &socket,
                                    node.node(),
                                    &get_peers_waiters,
                                )
                                .await;
                                interval.tick().await;
                            }
                        });
                    }
                }
            }
        } else if let GetPeersResult::NotFound(nodes) = result {
            for new_node in nodes.nodes().unique() {
                let _ = find_node.try_send(new_node.into());
            }
        } else {
            trace!("Unknown nodes response from: {:?}", addr);
        }
    }

    pub async fn find_node(&mut self, target_addr: &SocketAddr) -> Result<(), std::io::Error> {
        debug!("Sending find_node to {}", target_addr);
        let random_target = DhtNodeId::random();
        let query = KRpc {
            transaction_id: &self.next_transaction_id.to_be_bytes(),
            body: KRpcBody::Query(DhtQuery::FindNode {
                id: &self.id,
                target: &random_target,
            }),
            version: None,
        };
        self.udp
            .send_to(&krpc::to_bytes(query).unwrap(), target_addr)
            .await?;
        self.next_transaction_id += 1;
        Ok(())
    }

    async fn reply_ping(
        &mut self,
        transaction_id: &[u8],
        addr: &SocketAddr,
    ) -> Result<(), std::io::Error> {
        debug!("Replying ping to {}", addr);
        let response = KRpc {
            transaction_id,
            body: KRpcBody::Response {
                response: DhtResponse::PingOrAnnouncePeer { id: &self.id },
            },
            version: None,
        };
        self.udp
            .send_to(&krpc::to_bytes(response).unwrap(), addr)
            .await?;
        Ok(())
    }

    async fn reply_find_node(
        &mut self,
        transaction_id: &[u8],
        target: &DhtNodeId,
        addr: &SocketAddr,
    ) -> Result<(), std::io::Error> {
        debug!("Replying find_node to {}", addr);
        let nearests = DhtNodeCompactListOwned::from(
            self.router
                .read()
                .await
                .nearests(target)
                .into_iter()
                .map(Arc::as_ref),
        );
        let response = KRpc {
            transaction_id,
            body: KRpcBody::Response {
                response: DhtResponse::FindNode {
                    id: &self.id,
                    nodes: (&nearests).into(),
                },
            },
            version: None,
        };
        self.udp
            .send_to(&krpc::to_bytes(response).unwrap(), addr)
            .await?;
        Ok(())
    }

    async fn reply_get_peers(
        &mut self,
        transaction_id: &[u8],
        info_hash: &InfoHash,
        addr: &SocketAddr,
    ) -> Result<(), std::io::Error> {
        debug!("Replying get_peers to {}", addr);
        let nearests = DhtNodeCompactListOwned::from(
            self.router
                .read()
                .await
                .nearests(info_hash)
                .into_iter()
                .map(Arc::as_ref),
        );
        let response = KRpc {
            transaction_id,
            body: KRpcBody::Response {
                response: DhtResponse::GetPeers {
                    id: &self.id,
                    result: GetPeersResult::NotFound((&nearests).into()),
                    token: &rand::random::<[u8; 5]>(),
                },
            },
            version: None,
        };
        self.udp
            .send_to(&krpc::to_bytes(response).unwrap(), addr)
            .await?;
        Ok(())
    }

    fn start_find_node_thread(
        myid: Arc<DhtNodeId>,
        socket: Arc<UdpSocket>,
        mut rt: mpsc::Receiver<DhtNode>,
    ) -> JoinHandle<()> {
        spawn(async move {
            let mut dedup_set: HashSet<SocketAddr> = HashSet::new();
            let mut last_clean: SystemTime = SystemTime::now();
            let mut next_transaction_id: u16 = 0;
            let mut interval = interval(Duration::from_millis(3));
            while let Some(node) = rt.recv().await {
                if last_clean + Duration::from_secs(5) < SystemTime::now() {
                    dedup_set.clear();
                    last_clean = SystemTime::now();
                }
                if dedup_set.contains(&node.addr) {
                    debug!("Deduped find_node to {}", node.addr);
                    continue;
                }
                dedup_set.insert(node.addr.clone());

                debug!("Sending find_node to {}", node.addr);
                let random_target = DhtNodeId::random();
                let query = KRpc {
                    transaction_id: &next_transaction_id.to_be_bytes(),
                    body: KRpcBody::Query(DhtQuery::FindNode {
                        id: &myid,
                        target: &random_target,
                    }),
                    version: None,
                };
                if let Err(err) = socket.send_to(&krpc::to_bytes(query).unwrap(), node.addr).await {
                    warn!("Failed to send find_node: {}", err)
                }
                next_transaction_id = next_transaction_id.overflowing_add(1).0;

                interval.tick().await;
            }
        })
    }

    fn start_lookup_peer_thread(
        myid: Arc<DhtNodeId>,
        router: Arc<RwLock<R>>,
        socket: Arc<UdpSocket>,
        peer_lookup_tasks: Arc<Mutex<HashMap<InfoHash, PeerLookupTask>>>,
        get_peers_waiters: Arc<Mutex<LruCache<GetPeersRequestIndex, InfoHash>>>,
        mut rx: mpsc::Receiver<PeerLookupTask>,
    ) -> JoinHandle<()> {
        spawn(async move {
            while let Some(mut task) = rx.recv().await {
                info!("Start lookup peer for {}", task.info_hash);
                let mut tasks = peer_lookup_tasks.lock().await;

                let task_entry = tasks.entry(task.info_hash.as_ref().clone());
                match task_entry {
                    Entry::Occupied(mut previous_task) => {
                        warn!("Duplicate task, merging cloest_nodes");
                        previous_task
                            .get_mut()
                            .cloest_nodes
                            .append(&mut task.cloest_nodes);
                    }
                    Entry::Vacant(vacant) => {
                        let info_hash = task.info_hash.clone();
                        let task = vacant.insert(task);
                        task.cloest_nodes.extend(
                            router
                                .read()
                                .await
                                .nearests(info_hash.as_ref())
                                .iter()
                                .map(|node| node.distence_order(info_hash.as_ref())),
                        );
                        for node in &task.cloest_nodes {
                            Self::send_get_peer(
                                &myid,
                                &task.info_hash,
                                &socket,
                                node.node(),
                                &get_peers_waiters,
                            )
                            .await
                        }
                        let peer_lookup_tasks = peer_lookup_tasks.clone();
                        spawn(async move {
                            sleep(Duration::from_secs(30)).await;
                            if peer_lookup_tasks
                                .lock()
                                .await
                                .remove(info_hash.as_ref())
                                .is_some()
                            {
                                warn!("{} peer lookup timeout", info_hash);
                            }
                        });
                    }
                }
            }
        })
    }

    fn start_route_maintaining_thread(
        route: Arc<RwLock<R>>,
        trackers: Vec<SocketAddr>,
        tx: mpsc::Sender<DhtNode>,
    ) -> JoinHandle<()> {
        spawn(async move {
            let route_lock = route;
            let mut bootstraped = false;
            loop {
                if !bootstraped && route_lock.read().await.nodes_count() < 5 {
                    for tracker in trackers.iter() {
                        if let Err(err) = tx.send(tracker.clone()).await {
                            error!("Failed to send find_node, small route table: {}", err)
                        }
                    }
                } else {
                    bootstraped = true;
                    let unheathy: Vec<_> = {
                        let mut route = route_lock.write().await;
                        route.clean_unheathy()
                    };
                    trace!("Found {} unheathy nodes", unheathy.len());
                    for node in unheathy {
                        if let Err(err) = tx.send(node.as_ref().clone()).await {
                            error!(
                                "Failed to send find_node, update unhealth route table: {}",
                                err
                            )
                        }
                    }
                }

                sleep(Duration::from_secs(10)).await
            }
        })
    }

    fn start_crawler_thread(route: Arc<RwLock<R>>, tx: mpsc::Sender<DhtNode>) -> JoinHandle<()> {
        spawn(async move {
            let route_lock = route;
            loop {
                if let Some(node) = { route_lock.read().await.pick_node().cloned() } {
                    trace!("Pick node {} to crawle", node);
                    let _ = tx.send(node.as_ref().clone()).await;
                }

                sleep(Duration::from_millis(10)).await
            }
        })
    }

    async fn send_get_peer(
        myid: &DhtNodeId,
        info_hash: &InfoHash,
        socket: &UdpSocket,
        node: &DhtNode,
        get_peers_waiters: &Arc<Mutex<LruCache<GetPeersRequestIndex, DhtNodeId>>>,
    ) {
        trace!("Sending get_peer to {}", node.addr);
        let transaction_id = rand::random::<u16>().to_be_bytes();
        let query = KRpc {
            transaction_id: &transaction_id,
            body: KRpcBody::Query(DhtQuery::GetPeers {
                id: &myid,
                info_hash,
            }),
            version: None,
        };
        if let Err(err) = socket
            .send_to(&krpc::to_bytes(query).unwrap(), node.addr)
            .await
        {
            warn!("Failed to send get_peer: {}", err);
            return;
        }
        get_peers_waiters.lock().await.insert(
            GetPeersRequestIndex::new_with_node(node, &transaction_id).unwrap(),
            info_hash.clone(),
        );
        debug!("Sended get_peer to {}", node.addr);
    }
}

fn verify_peers(peers: &Vec<&krpc::DhtPeerCompact>) -> bool {
    peers.is_empty()
        || peers.iter().any(|p| match p.addr() {
            SocketAddr::V4(v4) => v4.ip().is_loopback() || v4.ip().is_unspecified(),
            SocketAddr::V6(_) => true,
        })
}

fn pop_n(task: &mut PeerLookupTask) -> Vec<OrderedNode> {
    let mut result = vec![];
    for _ in 0..8 {
        let node = task.cloest_nodes.pop();
        if let Some(node) = node {
            result.push(node)
        } else {
            break;
        }
    }
    result
}
