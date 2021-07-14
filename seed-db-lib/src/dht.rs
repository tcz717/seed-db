use itertools::Itertools;
use log::{debug, error, info, trace, warn};
use serde::de::Visitor;
use serde::{Deserialize, Serialize, Serializer};
use tokio::net::UdpSocket;
use tokio::net::{lookup_host, ToSocketAddrs};
use tokio::sync::{mpsc, RwLock};
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tokio::{join, spawn};

use std::convert::TryInto;
use std::fmt::{Debug, Display};
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use crate::dht::krpc::{
    DhtNodeCompactListOwned, DhtQuery, DhtResponse, GetPeersResult, KRpc, KRpcBody,
};

pub mod krpc;
mod utils;

#[repr(transparent)]
#[derive(PartialEq, Eq, PartialOrd, Ord, Default, Clone, Hash)]
pub struct DhtNodeId([u8; 20]);

impl DhtNodeId {
    pub const BITS: u32 = u8::BITS * 20;
    pub fn new(bytes: &[u8; 20]) -> Self {
        DhtNodeId(bytes.clone())
    }
    pub fn zered() -> Self {
        Default::default()
    }

    pub fn max() -> Self {
        DhtNodeId([u8::MAX; 20])
    }

    pub fn random() -> Self {
        DhtNodeId(rand::random())
    }

    pub fn traverse_bits(&self) -> DhtNodeIdBitIter<'_> {
        DhtNodeIdBitIter::new(self)
    }

    pub fn set(&mut self, idx: usize) {
        const WIDTH: usize = u8::BITS as usize;
        let bit = 1 << (idx % WIDTH);
        let byte = 19 - idx / WIDTH;
        self.0[byte] |= bit;
    }
    pub fn unset(&mut self, idx: usize) {
        const WIDTH: usize = u8::BITS as usize;
        let bit = 1 << (idx % WIDTH);
        let byte = 19 - idx / WIDTH;
        self.0[byte] &= !bit;
    }

    pub fn bit(&self, idx: usize) -> bool {
        const WIDTH: usize = u8::BITS as usize;
        let bit = 1 << (idx % WIDTH);
        let byte = 19 - idx / WIDTH;
        self.0[byte] & bit > 0
    }

    pub fn write(&mut self, idx: usize, val: bool) {
        const WIDTH: usize = u8::BITS as usize;
        let bit = 1 << (idx % WIDTH);
        let byte = 19 - idx / WIDTH;
        self.0[byte] &= !bit;
        self.0[byte] |= if val { 1 << (idx % WIDTH) } else { 0 }
    }
}

impl std::ops::BitXor for &DhtNodeId {
    type Output = DhtNodeId;

    fn bitxor(self, rhs: Self) -> Self::Output {
        let mut result = DhtNodeId::default();

        for (i, byte) in result.0.iter_mut().enumerate() {
            *byte = self.0[i] ^ rhs.0[i];
        }
        result
    }
}

impl Debug for DhtNodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for i in self.0 {
            write!(f, "{:02X} ", i)?
        }
        Ok(())
    }
}

impl Display for DhtNodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", base64::encode(self.0))
    }
}

impl AsRef<[u8; 20]> for DhtNodeId {
    fn as_ref(&self) -> &[u8; 20] {
        &self.0
    }
}

impl Into<DhtNodeId> for &[u8; 20] {
    fn into(self) -> DhtNodeId {
        DhtNodeId::new(self)
    }
}

impl Serialize for DhtNodeId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_bytes(&self.0)
    }
}

impl<'de> Deserialize<'de> for DhtNodeId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct DhtNodeIdVisitor;
        impl<'de> Visitor<'de> for DhtNodeIdVisitor {
            type Value = DhtNodeId;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("20 bytes Node ID")
            }
            fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(DhtNodeId::new(v.try_into().map_err(|_| {
                    serde::de::Error::invalid_length(v.len(), &self)
                })?))
            }
        }
        deserializer.deserialize_bytes(DhtNodeIdVisitor)
    }
}

impl<'a, 'de: 'a> Deserialize<'de> for &'a DhtNodeId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct DhtNodeIdVisitor;
        impl<'de> Visitor<'de> for DhtNodeIdVisitor {
            type Value = &'de DhtNodeId;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("20 bytes Node ID")
            }
            fn visit_borrowed_bytes<E>(self, v: &'de [u8]) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                let bytes = TryInto::<&[u8; 20]>::try_into(v)
                    .map_err(|_| serde::de::Error::invalid_length(v.len(), &self))?;
                Ok(unsafe { &*(bytes.as_ptr() as *const _) })
            }
        }
        deserializer.deserialize_bytes(DhtNodeIdVisitor)
    }
}

pub struct DhtNodeIdBitIter<'a> {
    id: &'a DhtNodeId,
    head: usize,
    tail: usize,
}

impl<'a> DhtNodeIdBitIter<'a> {
    pub fn new(id: &'a DhtNodeId) -> Self {
        Self {
            id,
            head: 0,
            tail: DhtNodeId::BITS as usize,
        }
    }
}

impl<'a> Iterator for DhtNodeIdBitIter<'a> {
    type Item = bool;

    fn next(&mut self) -> Option<Self::Item> {
        if self.head < self.tail {
            let bit = Some(self.id.bit(self.head));
            self.head += 1;
            bit
        } else {
            None
        }
    }
}
impl<'a> DoubleEndedIterator for DhtNodeIdBitIter<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.head < self.tail {
            self.tail -= 1;
            Some(self.id.bit(self.head))
        } else {
            None
        }
    }
}

pub type InfoHash = DhtNodeId;

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct DhtNode {
    pub id: DhtNodeId,
    pub addr: SocketAddr,
}

impl DhtNode {
    pub fn new(id: DhtNodeId, addr: SocketAddr) -> Self {
        Self { id: id, addr }
    }

    /// Get a reference to the dht node's id.
    pub fn id(&self) -> &DhtNodeId {
        &self.id
    }
}

impl Display for DhtNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Node<{}, {}>", self.id, self.addr)
    }
}

pub type SharedDhtNode = Arc<DhtNode>;

pub trait RouteTable {
    fn id(&self) -> &DhtNodeId;
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
    id: DhtNodeId,
    /// To be removed in the future
    seeds: std::collections::HashSet<InfoHash>,
}

pub struct DhtClientController {
    main: JoinHandle<()>,
    route_maintaining: JoinHandle<()>,
    find_node_sender: JoinHandle<()>,
    crawler: JoinHandle<()>,
}

impl DhtClientController {
    pub async fn stop(self) {
        let _ = join!(
            self.main,
            self.route_maintaining,
            self.find_node_sender,
            self.crawler
        );
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
            seeds: Default::default(),
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
        let (find_node, rx) = mpsc::channel::<SocketAddr>(128);
        let route_maintaining = Self::start_route_maintaining_thread(
            self.router.clone(),
            self.trackers.clone(),
            find_node.clone(),
        );
        let find_node_sender = Self::start_find_node_thread(self.id.clone(), self.udp.clone(), rx);
        let crawler = Self::start_crawler_thread(self.router.clone(), find_node.clone());

        let main = spawn(async move {
            let mut buf = vec![0_u8; 4 * 1024];
            loop {
                match self.udp.recv_from(&mut *buf).await {
                    Ok((len, addr)) => {
                        debug!("{:?} bytes received from {:?}", len, addr);

                        if let Ok(packet) = krpc::from_bytes(&buf[0..len]) {
                            // println!("{:#?} packet received", packet);
                            if let Some(id) = packet.node_id() {
                                let node = DhtNode::new(id.clone(), addr.clone());
                                trace!("Updating Node {}", &node);
                                self.router.write().await.update(node);
                            }
                            self.process_packet(packet, addr, &find_node).await;
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
            main,
            route_maintaining,
            find_node_sender,
            crawler,
        };
    }

    async fn process_packet(
        &mut self,
        packet: KRpc<'_>,
        addr: SocketAddr,
        find_node: &mpsc::Sender<SocketAddr>,
    ) {
        match packet {
            KRpc {
                transaction_id,
                body: KRpcBody::Query(query),
                ..
            } => {
                if let Err(err) = match query {
                    DhtQuery::AnnouncePeer { info_hash, .. } => {
                        self.seeds.insert(info_hash.clone());
                        info!(
                            "Got seed hash: {} ({} in total)",
                            info_hash,
                            self.seeds.len()
                        );
                        self.reply_ping(transaction_id, &addr).await
                    }
                    DhtQuery::FindNode { target, .. } => {
                        self.reply_find_node(transaction_id, target, &addr).await
                    }
                    DhtQuery::GetPeers { info_hash, .. } => {
                        self.seeds.insert(info_hash.clone());
                        info!(
                            "Got seed hash: {} ({} in total)",
                            info_hash,
                            self.seeds.len()
                        );
                        self.reply_get_peers(transaction_id, info_hash, &addr).await
                    }
                    DhtQuery::Ping { .. } => self.reply_ping(transaction_id, &addr).await,
                } {
                    error!("Failed to send reply: {}", err)
                }
            }
            KRpc {
                body: KRpcBody::Response { response },
                ..
            } => match response {
                DhtResponse::FindNode { nodes, .. } => {
                    for new_node in nodes.nodes().unique() {
                        let _ = find_node.try_send(new_node.addr());
                    }
                }
                DhtResponse::GetPeers {
                    id: _,
                    token: _,
                    result: _,
                } => (),
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
        debug!("Replying find_node to {}", addr);
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
        myid: DhtNodeId,
        socket: Arc<UdpSocket>,
        mut rt: mpsc::Receiver<SocketAddr>,
    ) -> JoinHandle<()> {
        spawn(async move {
            let mut next_transaction_id: u16 = 0;
            while let Some(addr) = rt.recv().await {
                debug!("Sending find_node to {}", addr);
                let random_target = DhtNodeId::random();
                let query = KRpc {
                    transaction_id: &next_transaction_id.to_be_bytes(),
                    body: KRpcBody::Query(DhtQuery::FindNode {
                        id: &myid,
                        target: &random_target,
                    }),
                    version: None,
                };
                if let Err(err) = socket.send_to(&krpc::to_bytes(query).unwrap(), addr).await {
                    warn!("Failed to send find_node: {}", err)
                }
                next_transaction_id = next_transaction_id.overflowing_add(1).0;
            }
        })
    }

    fn start_route_maintaining_thread(
        route: Arc<RwLock<R>>,
        trackers: Vec<SocketAddr>,
        tx: mpsc::Sender<SocketAddr>,
    ) -> JoinHandle<()> {
        spawn(async move {
            let route_lock = route;
            let mut bootstraped = false;
            loop {
                if !bootstraped && route_lock.read().await.nodes_count() < 5 {
                    for tracker in trackers.clone().iter() {
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
                    info!("Found {} unheathy nodes", unheathy.len());
                    for node in unheathy {
                        if let Err(err) = tx.send(node.addr.clone()).await {
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

    fn start_crawler_thread(route: Arc<RwLock<R>>, tx: mpsc::Sender<SocketAddr>) -> JoinHandle<()> {
        spawn(async move {
            let route_lock = route;
            loop {
                if let Some(node) = { route_lock.read().await.pick_node().cloned() } {
                    trace!("Pick node {} to crawle", node);
                    let _ = tx.send(node.addr.clone()).await;
                }

                sleep(Duration::from_millis(200)).await
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::dht::DhtNodeId;

    #[test]
    fn dhtnodeid_ord() {
        let mut small = [0; 20];
        small[1] = 1;
        let mut big = [0; 20];
        big[0] = 1;
        assert!(DhtNodeId(small) < DhtNodeId(big))
    }

    #[test]
    fn dhtnodeid_write() {
        let mut origin = DhtNodeId([0b111; 20]);
        let mut expected = [0b111; 20];
        expected[0] = 0b101;
        origin.write(153, false);
        assert_eq!(origin, DhtNodeId(expected));
    }
}
