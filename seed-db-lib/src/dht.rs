use bendy::value::Value;
use bytes::BytesMut;
use futures::StreamExt;
use serde::{Deserialize, Serialize, Serializer};
use std::collections::HashMap;
use std::fmt::{Debug, Display};
use std::net::UdpSocket as StdUdpSocket;
use std::net::{Ipv4Addr, SocketAddr};
use std::ops::{Add, Shl, Shr};
use tokio::io;
use tokio::net::UdpSocket;
use tokio::net::{lookup_host, ToSocketAddrs};
use tokio_util::codec::Decoder;
use tokio_util::udp::UdpFramed;

use crate::bencode::BencodeConverter;

#[derive(PartialEq, Eq, PartialOrd, Ord, Default, Clone, Serialize, Deserialize)]
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

impl Shr<usize> for &DhtNodeId {
    type Output = DhtNodeId;

    fn shr(self, rhs: usize) -> Self::Output {
        const WIDTH: usize = u8::BITS as usize;
        const LEN: usize = 20;
        let shift = rhs % WIDTH;
        let limbshift = rhs / WIDTH;
        let mut res = self.clone();
        for i in (limbshift + 1..LEN).rev() {
            res.0[i] =
                (res.0[i - limbshift] >> shift) | (res.0[i - 1 - limbshift] << (WIDTH - shift));
        }
        res.0[limbshift] = res.0[0] >> shift;
        res.0[..limbshift - 1].fill(0);
        res
    }
}

impl Add for &DhtNodeId {
    type Output = DhtNodeId;

    fn add(self, rhs: Self) -> Self::Output {
        let mut res: Self::Output = Default::default();
        let mut carry = 0;
        for i in (0..20).rev() {
            let sum = (self.0[i] as u16) + (rhs.0[i] as u16 + carry);
            res.0[i] = (sum % 256) as u8;
            carry = sum / 256;
        }
        res
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

#[derive(Debug)]
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
    udp: UdpSocket,
    trackers: Vec<SocketAddr>,
    router: R,
    next_transaction_id: u16,
}

impl<R: RouteTable + Default> DhtClient<R> {
    pub fn new() -> Result<Self, std::io::Error> {
        let socket: StdUdpSocket = StdUdpSocket::bind((Ipv4Addr::UNSPECIFIED, 5717))?;
        let socket: UdpSocket = UdpSocket::from_std(socket)?;
        Ok(DhtClient {
            udp: socket,
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
        for tracker in trackers {
            if let Ok(addrs) = lookup_host(tracker).await {
                self.trackers.extend(addrs);
            }
        }
        self.trackers.len() - old_size
    }

    pub async fn run(&mut self) -> Result<(), std::io::Error> {
        let mut buf: [u8; 1024] = [0; 1024];
        loop {
            let (len, addr) = self.udp.recv_from(&mut buf).await?;
            println!("{:?} bytes received from {:?}", len, addr);

            if let Ok(packet) = bendy::serde::from_bytes::<'_, KRpc<'_>>(&buf[..len]) {
                println!("{:#?} packet received", packet);
            } else {
                println!("Unknown packet received from {:?}", addr);
            }

            let len = self.udp.send_to(&buf[..len], addr).await?;
            println!("{:?} bytes sent", len);
        }
    }

    pub async fn find_node(&mut self, target_addr: &SocketAddr) -> Result<(), std::io::Error> {
        let random_target = DhtNodeId::max();
        let query = KRpc::Query {
            transaction_id: &self.next_transaction_id.to_be_bytes(),
            query: DhtQuery::FindNode {
                id: self.router.id().as_ref(),
                target: random_target.as_ref(),
            },
        };
        self.udp
            .send_to(&bendy::serde::to_bytes(&query).unwrap(), target_addr)
            .await?;
        self.next_transaction_id += 1;
        Ok(())
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
        #[serde(with = "serde_bytes")]
        id: &'a [u8],
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
        #[serde(with = "serde_bytes")]
        id: &'a [u8],
        #[serde(with = "serde_bytes")]
        target: &'a [u8],
    },
    GetPeers {
        #[serde(with = "serde_bytes")]
        id: &'a [u8],
        #[serde(with = "serde_bytes")]
        info_hash: &'a [u8],
    },
    Ping {
        #[serde(with = "serde_bytes")]
        id: &'a [u8],
    },
}
#[derive(Serialize, Deserialize, PartialEq, Debug)]
#[serde(untagged)]
pub enum DhtResponse<'a> {
    FindNode {
        #[serde(with = "serde_bytes")]
        id: &'a [u8],
        #[serde(with = "serde_bytes")]
        nodes: &'a [u8],
    },
    GetPeers {
        #[serde(with = "serde_bytes")]
        id: &'a [u8],
        token: &'a str,
        #[serde(flatten)]
        result: GetPeersResult<'a>,
    },
    PingOrAnnouncePeer {
        #[serde(with = "serde_bytes")]
        id: &'a [u8],
    },
}
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub enum GetPeersResult<'a> {
    #[serde(rename = "values")]
    Found(Vec<&'a [u8]>),
    #[serde(rename = "nodes")]
    #[serde(with = "serde_bytes")]
    NotFound(&'a [u8]),
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

#[cfg(test)]
mod tests {
    use crate::dht::{DhtNodeId, DhtQuery, DhtResponse, GetPeersResult, KRpc};

    #[test]
    fn deserialize_ping_query() {
        let query = KRpc::Query {
            transaction_id: b"aa",
            query: DhtQuery::Ping {
                id: b"abcdefghij0123456789",
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
                id: b"abcdefghij0123456789",
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
                id: b"abcdefghij0123456789",
                result: GetPeersResult::Found(vec![b"axje.u", b"idhtnm"]),
                token: "aoeusnth",
            },
        };
        let bytes = bendy::serde::to_bytes(&query).unwrap();

        println!("{}", std::str::from_utf8(&bytes).unwrap());
        assert_eq!(
            bytes,
            "d1:rd2:id20:abcdefghij01234567895:token8:aoeusnth6:valuesl6:axje.u6:idhtnmee1:t2:aa1:y1:re".as_bytes()
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
    fn dhtnodeid_shr() {
        let num = 10934272u32;
        let origin = DhtNodeId({
            let mut tmp = [0; 20];
            tmp[..4].copy_from_slice(&num.to_be_bytes());
            tmp
        });

        let moved = DhtNodeId({
            let mut tmp = [0; 20];
            tmp[..4].copy_from_slice(&(num >> 10).to_be_bytes());
            tmp
        });

        assert_eq!(&origin >> 10, moved);
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
