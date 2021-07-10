use itertools::Itertools;
use log::{debug, error, info, warn};
use serde::de::Visitor;
use serde::{Deserialize, Serialize, Serializer};
use tokio::spawn;
use tokio::sync::mpsc;

use std::convert::TryInto;
use std::fmt::{Debug, Display};
use std::net::UdpSocket as StdUdpSocket;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use tokio::net::UdpSocket;
use tokio::net::{lookup_host, ToSocketAddrs};

use crate::dht::krpc::{DhtNodeCompactListOwned, DhtQuery, DhtResponse, GetPeersResult, KRpc};

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

pub type InfoHash = DhtNodeId;

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct DhtNode {
    pub id: Box<DhtNodeId>,
    pub addr: SocketAddr,
}

impl DhtNode {
    pub fn new(id: DhtNodeId, addr: SocketAddr) -> Self {
        Self {
            id: Box::new(id),
            addr,
        }
    }

    /// Get a reference to the dht node's id.
    pub fn id(&self) -> &DhtNodeId {
        self.id.as_ref()
    }
}

pub trait RouteTable {
    fn id(&self) -> &DhtNodeId;
    fn update(&mut self, node: DhtNode);
    fn nearests(&self, id: &DhtNodeId) -> Vec<&DhtNode>;
    fn unheathy(&self) -> Vec<&DhtNode>;
    fn clean_unheathy(&mut self) -> Vec<&DhtNode>;
}

pub struct DhtClient<R: RouteTable> {
    udp: Arc<UdpSocket>,
    trackers: Vec<SocketAddr>,
    router: R,
    next_transaction_id: u16,
}

impl<R: RouteTable + Default> DhtClient<R> {
    pub fn new() -> Result<Self, std::io::Error> {
        let socket: StdUdpSocket = StdUdpSocket::bind((Ipv4Addr::UNSPECIFIED, 5717))?;
        let socket: UdpSocket = UdpSocket::from_std(socket)?;
        Ok(DhtClient {
            udp: socket.into(),
            trackers: vec![],
            router: Default::default(),
            next_transaction_id: Default::default(),
        })
    }
}

impl<R: RouteTable> DhtClient<R> {
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

    pub async fn run(&mut self) -> Result<(), std::io::Error> {
        let mut buf: [u8; 1024] = [0; 1024];
        let (find_node, rx) = mpsc::channel::<SocketAddr>(128);
        Self::start_find_node_thread(self.router.id().clone(), self.udp.clone(), rx);

        for tracker in self.trackers.clone().iter() {
            self.find_node(tracker).await?;
        }

        loop {
            let (len, addr) = self.udp.recv_from(&mut buf).await?;
            debug!("{:?} bytes received from {:?}", len, addr);

            if let Ok(packet) = krpc::from_bytes(&buf, len) {
                // println!("{:#?} packet received", packet);
                if let Some(id) = packet.node_id() {
                    let node = DhtNode::new(id.clone(), addr.clone());
                    info!("Updating Node {:?}", &node);
                    self.router.update(node);
                }

                match packet {
                    KRpc::Query {
                        transaction_id,
                        query,
                    } => {
                        if let Err(err) = match query {
                            DhtQuery::AnnouncePeer {
                                implied_port,
                                info_hash,
                                port,
                                token,
                                ..
                            } => self.reply_ping(transaction_id, &addr).await,
                            DhtQuery::FindNode { target, .. } => {
                                self.reply_find_node(transaction_id, target, &addr).await
                            }
                            DhtQuery::GetPeers { info_hash, .. } => {
                                self.reply_get_peers(transaction_id, info_hash, &addr).await
                            }
                            DhtQuery::Ping { .. } => self.reply_ping(transaction_id, &addr).await,
                        } {
                            error!("Failed to send reply: {}", err)
                        }
                    }
                    KRpc::Response { response, .. } => match response {
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
                    KRpc::Error { error, .. } => {
                        warn!("Received error package <{:?}> from {}", error, addr)
                    }
                };
            } else {
                warn!("Unknown packet received from {:?}", addr);
            }
        }
    }

    pub async fn find_node(&mut self, target_addr: &SocketAddr) -> Result<(), std::io::Error> {
        info!("Sending find_node to {}", target_addr);
        let random_target = DhtNodeId::random();
        let query = KRpc::Query {
            transaction_id: &self.next_transaction_id.to_be_bytes(),
            query: DhtQuery::FindNode {
                id: self.router.id(),
                target: &random_target,
            },
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
        info!("Replying ping to {}", addr);
        let query = KRpc::Response {
            transaction_id,
            response: DhtResponse::PingOrAnnouncePeer {
                id: self.router.id(),
            },
        };
        self.udp
            .send_to(&krpc::to_bytes(query).unwrap(), addr)
            .await?;
        Ok(())
    }

    async fn reply_find_node(
        &mut self,
        transaction_id: &[u8],
        target: &DhtNodeId,
        addr: &SocketAddr,
    ) -> Result<(), std::io::Error> {
        info!("Replying find_node to {}", addr);
        let nearests = DhtNodeCompactListOwned::from(self.router.nearests(target).into_iter());
        let query = KRpc::Response {
            transaction_id,
            response: DhtResponse::FindNode {
                id: self.router.id(),
                nodes: (&nearests).into(),
            },
        };
        self.udp
            .send_to(&krpc::to_bytes(query).unwrap(), addr)
            .await?;
        Ok(())
    }

    async fn reply_get_peers(
        &mut self,
        transaction_id: &[u8],
        info_hash: &InfoHash,
        addr: &SocketAddr,
    ) -> Result<(), std::io::Error> {
        info!("Replying find_node to {}", addr);
        let nearests = DhtNodeCompactListOwned::from(self.router.nearests(info_hash).into_iter());
        let query = KRpc::Response {
            transaction_id,
            response: DhtResponse::GetPeers {
                id: self.router.id(),
                result: GetPeersResult::NotFound((&nearests).into()),
                token: &rand::random::<[u8; 5]>(),
            },
        };
        self.udp
            .send_to(&krpc::to_bytes(query).unwrap(), addr)
            .await?;
        Ok(())
    }

    fn start_find_node_thread(
        myid: DhtNodeId,
        socket: Arc<UdpSocket>,
        mut rt: mpsc::Receiver<SocketAddr>,
    ) {
        spawn(async move {
            let mut next_transaction_id: u16 = 0;
            while let Some(addr) = rt.recv().await {
                info!("Sending find_node to {}", addr);
                let random_target = DhtNodeId::random();
                let query = KRpc::Query {
                    transaction_id: &next_transaction_id.to_be_bytes(),
                    query: DhtQuery::FindNode {
                        id: &myid,
                        target: &random_target,
                    },
                };
                if let Err(err) = socket.send_to(&krpc::to_bytes(query).unwrap(), addr).await {
                    warn!("Failed to send find_node: {}", err)
                }
                next_transaction_id += 1;
            }
        });
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
