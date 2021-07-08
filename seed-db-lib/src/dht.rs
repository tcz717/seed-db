use itertools::Itertools;
use serde::de::Visitor;
use serde::{Deserialize, Serialize, Serializer};
use tokio::spawn;
use tokio::sync::mpsc;

use std::convert::{TryFrom, TryInto};
use std::fmt::{Debug, Display};
use std::mem::size_of;
use std::net::UdpSocket as StdUdpSocket;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use tokio::net::UdpSocket;
use tokio::net::{lookup_host, ToSocketAddrs};

use self::utils::{BytesSliceVisitor, BytesVisitor};

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

#[derive(Debug, PartialEq, Eq, Hash)]
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
}

pub trait RouteTable {
    fn id(&self) -> &DhtNodeId;
    fn update(&mut self, node: DhtNode);
    fn get_nearests<I>(&self, id: &DhtNodeId) -> I
    where
        I: Iterator<Item = DhtNode>;
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
        Self::find_node_handler(self.router.id().clone(), self.udp.clone(), rx);

        for tracker in self.trackers.clone().iter() {
            self.find_node(tracker).await?;
        }

        loop {
            let (len, addr) = self.udp.recv_from(&mut buf).await?;
            println!("{:?} bytes received from {:?}", len, addr);

            if let Ok(packet) = bendy::serde::from_bytes::<'_, KRpc<'_>>(&buf[..len]) {
                println!("{:#?} packet received", packet);
                match packet {
                    KRpc::Query {
                        transaction_id,
                        query,
                    } => todo!(),
                    KRpc::Response {
                        transaction_id,
                        response,
                    } => match response {
                        DhtResponse::FindNode { id, nodes } => {
                            let node = DhtNode::new(id.clone(), addr.clone());
                            println!("Updating Node {:?}", &node);
                            self.router.update(node);

                            for new_node in nodes.nodes().unique() {
                                let _ = find_node.try_send(new_node.addr());
                            }
                        }
                        DhtResponse::GetPeers { id, token, result } => todo!(),
                        DhtResponse::PingOrAnnouncePeer { id } => {}
                    },
                    _ => (),
                }
            } else {
                println!("Unknown packet received from {:?}", addr);
            }
        }
    }

    pub async fn find_node(&mut self, target_addr: &SocketAddr) -> Result<(), std::io::Error> {
        println!("Sending find_node to {}", target_addr);
        let random_target = DhtNodeId::random();
        let query = KRpc::Query {
            transaction_id: &self.next_transaction_id.to_be_bytes(),
            query: DhtQuery::FindNode {
                id: self.router.id(),
                target: random_target.as_ref(),
            },
        };
        self.udp
            .send_to(&bendy::serde::to_bytes(&query).unwrap(), target_addr)
            .await?;
        self.next_transaction_id += 1;
        Ok(())
    }

    fn find_node_handler(
        myid: DhtNodeId,
        socket: Arc<UdpSocket>,
        mut rt: mpsc::Receiver<SocketAddr>,
    ) {
        spawn(async move {
            let mut next_transaction_id: u16 = 0;
            while let Some(addr) = rt.recv().await {
                println!("Sending find_node to {}", addr);
                let random_target = DhtNodeId::random();
                let query = KRpc::Query {
                    transaction_id: &next_transaction_id.to_be_bytes(),
                    query: DhtQuery::FindNode {
                        id: &myid,
                        target: random_target.as_ref(),
                    },
                };
                if let Err(err) = socket
                    .send_to(&bendy::serde::to_bytes(&query).unwrap(), addr)
                    .await
                {
                    println!("Failed to send find_node: {}", err)
                }
                next_transaction_id += 1;
            }
        });
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
#[serde(tag = "y")]
pub enum KRpc<'a> {
    #[serde(rename = "q")]
    Query {
        #[serde(rename = "t")]
        #[serde(with = "serde_bytes")]
        transaction_id: &'a [u8],
        #[serde(flatten)]
        query: DhtQuery<'a>,
    },
    #[serde(rename = "r")]
    Response {
        #[serde(rename = "t")]
        #[serde(with = "serde_bytes")]
        transaction_id: &'a [u8],
        #[serde(rename = "r")]
        response: DhtResponse<'a>,
    },
    #[serde(rename = "e")]
    Error {
        #[serde(rename = "t")]
        #[serde(with = "serde_bytes")]
        transaction_id: &'a [u8],
        #[serde(rename = "e")]
        error: (i32, &'a str),
    },
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "q", content = "a")]
pub enum DhtQuery<'a> {
    AnnouncePeer {
        id: &'a DhtNodeId,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        #[serde(serialize_with = "DhtQuery::serialize_implied_port")]
        implied_port: Option<bool>,
        #[serde(with = "serde_bytes")]
        info_hash: &'a [u8],
        port: u16,
        token: &'a str,
    },
    FindNode {
        id: &'a DhtNodeId,
        #[serde(with = "serde_bytes")]
        target: &'a [u8],
    },
    GetPeers {
        id: &'a DhtNodeId,
        #[serde(with = "serde_bytes")]
        info_hash: &'a [u8],
    },
    Ping {
        id: &'a DhtNodeId,
    },
}
#[derive(Serialize, Deserialize, PartialEq, Debug)]
#[serde(untagged)]
pub enum DhtResponse<'a> {
    FindNode {
        id: &'a DhtNodeId,
        nodes: DhtNodeCompactList<'a>,
    },
    GetPeers {
        id: &'a DhtNodeId,
        token: &'a str,
        #[serde(flatten)]
        result: GetPeersResult<'a>,
    },
    PingOrAnnouncePeer {
        id: &'a DhtNodeId,
    },
}
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub enum GetPeersResult<'a> {
    #[serde(rename = "values")]
    Found(Vec<DhtNodeId>),
    #[serde(rename = "nodes")]
    #[serde(borrow)]
    NotFound(DhtNodeCompactList<'a>),
}

impl<'a> DhtQuery<'a> {
    fn serialize_implied_port<S>(val: &Option<bool>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match val {
            Some(b) => serializer.serialize_bool(*b),
            None => unimplemented!(),
        }
    }
}

#[derive(PartialEq)]
pub struct DhtNodeCompactList<'a>(&'a [u8]);

impl<'a> DhtNodeCompactList<'a> {
    pub fn nodes(&self) -> impl Iterator<Item = &DhtNodeCompact> {
        self.0
            .chunks_exact(size_of::<DhtNodeCompact>())
            .map(|bytes| unsafe { &*(bytes.as_ptr() as *const _) })
    }
}

impl<'a> Debug for DhtNodeCompactList<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list().entries(self.nodes()).finish()
    }
}

impl<'a> Serialize for DhtNodeCompactList<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_bytes(&self.0)
    }
}

impl<'de: 'a, 'a> Deserialize<'de> for DhtNodeCompactList<'a> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let bytes = deserializer.deserialize_bytes(BytesSliceVisitor)?;
        if bytes.len() % size_of::<DhtNodeCompact>() != 0 {
            Err(serde::de::Error::invalid_length(
                bytes.len(),
                &"Bytes size must be n times of DhtNodeCompact",
            ))
        } else {
            Ok(DhtNodeCompactList(bytes))
        }
    }
}

#[derive(PartialEq, Eq, Hash)]
pub struct DhtNodeCompact([u8; 26]);

impl DhtNodeCompact {
    fn addr(&self) -> SocketAddr {
        SocketAddr::from((
            Ipv4Addr::from(<&[u8; 4]>::try_from(&self.0[20..24]).unwrap().to_owned()),
            u16::from_be_bytes(self.0[24..26].try_into().unwrap()),
        ))
    }
}

impl Into<DhtNode> for &DhtNodeCompact {
    fn into(self) -> DhtNode {
        DhtNode {
            id: Box::new(DhtNodeId::new(self.0[0..20].try_into().unwrap())),
            addr: SocketAddr::from((
                Ipv4Addr::from(<&[u8; 4]>::try_from(&self.0[20..24]).unwrap().to_owned()),
                u16::from_be_bytes(self.0[24..26].try_into().unwrap()),
            )),
        }
    }
}

impl Debug for DhtNodeCompact {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DhtNodeCompact")
            .field("id", &base64::encode(&self.0[0..20]))
            .field(
                "addr",
                &SocketAddr::from((
                    Ipv4Addr::from(<&[u8; 4]>::try_from(&self.0[20..24]).unwrap().to_owned()),
                    u16::from_be_bytes(self.0[24..26].try_into().unwrap()),
                )),
            )
            .finish()
    }
}

impl Serialize for DhtNodeCompact {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_bytes(&self.0)
    }
}

impl<'de> Deserialize<'de> for DhtNodeCompact {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer
            .deserialize_bytes(BytesVisitor::<26>)
            .map(DhtNodeCompact)
    }
}

impl<'de: 'a, 'a> Deserialize<'de> for &'a DhtNodeCompact {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer
            .deserialize_bytes(BytesVisitor::<26>)
            .map(|bytes| unsafe { &*(bytes.as_ptr() as *const _) })
    }
}

#[cfg(test)]
mod tests {
    use crate::dht::{DhtNodeId, DhtQuery, DhtResponse, GetPeersResult, KRpc};

    #[test]
    fn deserialize_ping_query() {
        let query = KRpc::Query {
            transaction_id: b"aa",
            query: DhtQuery::Ping {
                id: &b"abcdefghij0123456789".into(),
            },
        };
        let bytes = bendy::serde::to_bytes(&query).unwrap();

        assert_eq!(
            bytes,
            "d1:ad2:id20:abcdefghij0123456789e1:q4:ping1:t2:aa1:y1:qe".as_bytes()
        );
    }

    #[test]
    fn deserialize_announce_peer_query() {
        let query = KRpc::Query {
            transaction_id: b"aa",
            query: DhtQuery::AnnouncePeer {
                id: &b"abcdefghij0123456789".into(),
                implied_port: Some(true),
                info_hash: b"mnopqrstuvwxyz123456",
                port: 6881,
                token: "aoeusnth",
            },
        };
        let bytes = bendy::serde::to_bytes(&query).unwrap();

        assert_eq!(
            bytes,
            "d1:ad2:id20:abcdefghij012345678912:implied_porti1e9:info_hash20:mnopqrstuvwxyz1234564:porti6881e5:token8:aoeusnthe1:q13:announce_peer1:t2:aa1:y1:qe".as_bytes()
        );
    }

    #[test]
    fn deserialize_get_peers_response() {
        let query = KRpc::Response {
            transaction_id: b"aa",
            response: DhtResponse::GetPeers {
                id: &b"abcdefghij0123456789".into(),
                result: GetPeersResult::Found(vec![
                    b"01234567890123456789".into(),
                    b"a1234567890123456789".into(),
                ]),
                token: "aoeusnth",
            },
        };
        let bytes = bendy::serde::to_bytes(&query).unwrap();

        println!("{}", std::str::from_utf8(&bytes).unwrap());
        assert_eq!(
            bytes,
            "d1:rd2:id20:abcdefghij01234567895:token8:aoeusnth6:valuesl20:0123456789012345678920:a1234567890123456789ee1:t2:aa1:y1:re".as_bytes()
        );
    }

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
