use bendy::value::Value;
use bytes::BytesMut;
use core::num::bignum::FullOps;
use serde::{Deserialize, Serialize, Serializer};
use std::collections::HashMap;
use std::fmt::{Debug, Display};
use std::net::UdpSocket as StdUdpSocket;
use std::net::{Ipv4Addr, SocketAddr};
use std::ops::{Add, Shl, Shr};
use tokio::io;
use tokio::net::UdpSocket;
use tokio_util::codec::Decoder;
use tokio_util::udp::UdpFramed;

use crate::bencode::BencodeConverter;

#[derive(PartialEq, Eq, PartialOrd, Ord, Default, Clone)]
pub struct DhtNodeId([u8; 20]);

impl DhtNodeId {
    pub fn zered() -> Self {
        Default::default()
    }

    pub fn max() -> Self {
        DhtNodeId([u8::MAX; 20])
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
            write!(f, "{:02X}", i)?
        }
        Ok(())
    }
}

impl Display for DhtNodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", base64::encode(self.0))
    }
}

pub struct DhtNode {
    pub id: Box<DhtNodeId>,
    pub addr: SocketAddr,
}

pub trait RouteTable {
    fn update(&mut self, node: DhtNode);
    fn get_nearests<I>(&self, id: &DhtNodeId) -> I
    where
        I: Iterator<Item = DhtNode>;
}

struct DhtClient {
    udp: UdpSocket,
}

impl DhtClient {
    pub fn new_from_trackers(trackers: ()) -> Result<DhtClient, std::io::Error> {
        let socket: StdUdpSocket = StdUdpSocket::bind((Ipv4Addr::UNSPECIFIED, 5717))?;
        let socket: UdpSocket = UdpSocket::from_std(socket)?;
        Ok(DhtClient { udp: socket })
    }

    pub async fn run(&mut self) -> Result<(), std::io::Error> {
        let mut buf: [u8; 1024] = [0; 1024];
        loop {
            let (len, addr) = self.udp.recv_from(&mut buf).await?;
            println!("{:?} bytes received from {:?}", len, addr);

            let len = self.udp.send_to(&buf[..len], addr).await?;
            println!("{:?} bytes sent", len);
        }
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
#[serde(tag = "y")]
pub enum KRpc<'a> {
    #[serde(rename = "q")]
    Query {
        #[serde(rename = "t")]
        transaction_id: &'a str,
        #[serde(flatten)]
        query: DhtQuery<'a>,
    },
    #[serde(rename = "r")]
    Response {
        #[serde(rename = "t")]
        transaction_id: &'a str,
        #[serde(rename = "r")]
        response: DhtResponse<'a>,
    },
    #[serde(rename = "e")]
    Error {
        #[serde(rename = "t")]
        transaction_id: &'a str,
        #[serde(rename = "e")]
        error: (i32, &'a str),
    },
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "q", content = "a")]
pub enum DhtQuery<'a> {
    AnnouncePeer {
        id: &'a str,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        #[serde(serialize_with = "DhtQuery::serialize_implied_port")]
        implied_port: Option<bool>,
        info_hash: &'a str,
        port: u16,
        token: &'a str,
    },
    FindNode {
        id: &'a str,
        info_hash: &'a str,
    },
    GetPeers {
        id: &'a str,
        target: &'a str,
    },
    Ping {
        id: &'a str,
    },
}
#[derive(Serialize, Deserialize, PartialEq, Debug)]
#[serde(untagged)]
pub enum DhtResponse<'a> {
    FindNode {
        id: &'a str,
        nodes: &'a str,
    },
    GetPeers {
        id: &'a str,
        token: &'a str,
        #[serde(flatten)]
        result: GetPeersResult<'a>,
    },
    PingOrAnnouncePeer {
        id: &'a str,
    },
}
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub enum GetPeersResult<'a> {
    #[serde(rename = "values")]
    Found(Vec<&'a str>),
    #[serde(rename = "nodes")]
    NotFound(&'a str),
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
            transaction_id: "aa",
            query: DhtQuery::Ping {
                id: "abcdefghij0123456789",
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
            transaction_id: "aa",
            query: DhtQuery::AnnouncePeer {
                id: "abcdefghij0123456789",
                implied_port: Some(true),
                info_hash: "mnopqrstuvwxyz123456",
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
            transaction_id: "aa",
            response: DhtResponse::GetPeers {
                id: "abcdefghij0123456789",
                result: GetPeersResult::Found(vec!["axje.u", "idhtnm"]),
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
}
