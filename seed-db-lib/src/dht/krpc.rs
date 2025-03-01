use super::{DhtNode, DhtNodeId, InfoHash};
use crate::utils::{BorrowedBytesVisitor, BytesSliceVisitor, BytesVisitor};
use serde::{Deserialize, Serialize, Serializer};
use std::convert::{TryFrom, TryInto};
use std::fmt::Debug;
use std::mem::size_of;
use std::net::{Ipv4Addr, SocketAddr};

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct KRpc<'a> {
    #[serde(rename = "t")]
    #[serde(with = "serde_bytes")]
    pub transaction_id: &'a [u8],
    #[serde(flatten)]
    pub body: KRpcBody<'a>,
    #[serde(rename = "v")]
    #[serde(deserialize_with = "KRpc::deserialize_version")]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub version: Option<&'a [u8]>,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
#[serde(tag = "y")]
pub enum KRpcBody<'a> {
    #[serde(rename = "q")]
    Query(DhtQuery<'a>),
    #[serde(rename = "r")]
    Response {
        #[serde(borrow)]
        #[serde(rename = "r")]
        response: DhtResponse<'a>,
    },
    #[serde(rename = "e")]
    Error {
        #[serde(borrow)]
        #[serde(rename = "e")]
        error: (i32, &'a str),
    },
}

impl<'a> KRpc<'a> {
    pub fn node_id(&self) -> Option<&DhtNodeId> {
        match &self.body {
            KRpcBody::Query(query) => match query {
                DhtQuery::AnnouncePeer { id, .. } => Some(id),
                DhtQuery::FindNode { id, .. } => Some(id),
                DhtQuery::GetPeers { id, .. } => Some(id),
                DhtQuery::Ping { id } => Some(id),
            },
            KRpcBody::Response { response, .. } => match response {
                DhtResponse::FindNode { id, .. } => Some(id),
                DhtResponse::GetPeers { id, .. } => Some(id),
                DhtResponse::PingOrAnnouncePeer { id } => Some(id),
            },
            KRpcBody::Error { .. } => None,
        }
    }
    fn deserialize_version<'de: 'b, 'b, D>(deserializer: D) -> Result<Option<&'b [u8]>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_bytes(BytesSliceVisitor).map(Some)
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "q", content = "a")]
pub enum DhtQuery<'a> {
    AnnouncePeer {
        id: &'a DhtNodeId,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        #[serde(with = "crate::utils::skip_none")]
        implied_port: Option<u32>,
        info_hash: &'a InfoHash,
        port: u16,
        #[serde(with = "serde_bytes")]
        token: &'a [u8],
    },
    FindNode {
        id: &'a DhtNodeId,
        target: &'a DhtNodeId,
    },
    GetPeers {
        id: &'a DhtNodeId,
        info_hash: &'a InfoHash,
    },
    Ping {
        id: &'a DhtNodeId,
    },
}
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
#[serde(untagged)]
pub enum DhtResponse<'a> {
    GetPeers {
        id: &'a DhtNodeId,
        #[serde(with = "serde_bytes")]
        token: &'a [u8],
        #[serde(flatten)]
        result: GetPeersResult<'a>,
    },
    FindNode {
        id: &'a DhtNodeId,
        nodes: DhtNodeCompactList<'a>,
    },
    PingOrAnnouncePeer {
        id: &'a DhtNodeId,
    },
}
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum GetPeersResult<'a> {
    #[serde(rename = "values")]
    Found(Vec<&'a DhtPeerCompact>),
    #[serde(rename = "nodes")]
    #[serde(borrow)]
    NotFound(DhtNodeCompactList<'a>),
}

#[derive(PartialEq, Clone)]
pub struct DhtNodeCompactList<'a>(&'a [u8]);

#[derive(PartialEq)]
pub struct DhtNodeCompactListOwned(Vec<u8>);

impl<'a, I> From<I> for DhtNodeCompactListOwned
where
    I: Iterator<Item = &'a DhtNode>,
{
    fn from(nodes: I) -> Self {
        let len = nodes.size_hint().0 * size_of::<DhtNodeCompact>();
        let mut data = Vec::with_capacity(len);
        for node in nodes {
            data.extend_from_slice(node.id().as_ref());
            match node.addr {
                SocketAddr::V4(socketv4) => {
                    data.extend_from_slice(&socketv4.ip().octets());
                    data.extend_from_slice(&socketv4.port().to_be_bytes());
                }
                SocketAddr::V6(_) => panic!("Ip V4 cant be stored to DhtNodeCompactListOwned"),
            }
        }
        Self(data)
    }
}

impl<'a> From<&'a DhtNodeCompactListOwned> for DhtNodeCompactList<'a> {
    fn from(owned: &'a DhtNodeCompactListOwned) -> Self {
        Self(&owned.0)
    }
}

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

#[repr(transparent)]
#[derive(PartialEq, Eq, Hash)]
pub struct DhtNodeCompact([u8; 26]);

impl DhtNodeCompact {
    pub fn addr(&self) -> SocketAddr {
        SocketAddr::from((
            Ipv4Addr::from(<&[u8; 4]>::try_from(&self.0[20..24]).unwrap().to_owned()),
            u16::from_be_bytes(self.0[24..26].try_into().unwrap()),
        ))
    }
}

impl From<&DhtNodeCompact> for DhtNode {
    fn from(val: &DhtNodeCompact) -> Self {
        DhtNode {
            id: DhtNodeId::new(val.0[0..20].try_into().unwrap()),
            addr: SocketAddr::from((
                Ipv4Addr::from(<&[u8; 4]>::try_from(&val.0[20..24]).unwrap().to_owned()),
                u16::from_be_bytes(val.0[24..26].try_into().unwrap()),
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
            .deserialize_bytes(BorrowedBytesVisitor::<26>)
            .map(|bytes| unsafe { &*(bytes.as_ptr() as *const _) })
    }
}

#[repr(transparent)]
#[derive(PartialEq, Eq, Hash)]
pub struct DhtPeerCompact([u8; 6]);

impl DhtPeerCompact {
    pub fn addr(&self) -> SocketAddr {
        SocketAddr::from((
            Ipv4Addr::from(<&[u8; 4]>::try_from(&self.0[0..4]).unwrap().to_owned()),
            u16::from_be_bytes(self.0[4..6].try_into().unwrap()),
        ))
    }
}

impl From<&DhtPeerCompact> for SocketAddr {
    fn from(val: &DhtPeerCompact) -> Self {
        val.addr()
    }
}

impl From<&[u8; 6]> for DhtPeerCompact {
    fn from(val: &[u8; 6]) -> Self {
        Self(val.clone())
    }
}

impl From<&SocketAddr> for DhtPeerCompact {
    fn from(val: &SocketAddr) -> Self {
        match val {
            SocketAddr::V4(socketv4) => {
                let mut bytes = [0u8; 6];
                bytes[0..4].copy_from_slice(&socketv4.ip().octets());
                bytes[4..6].copy_from_slice(&socketv4.port().to_be_bytes());
                Self(bytes)
            }
            SocketAddr::V6(_) => panic!("Ip V4 cant be stored to DhtPeerCompact"),
        }
    }
}

impl Debug for DhtPeerCompact {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DhtPeerCompact")
            .field(
                "addr",
                &SocketAddr::from((
                    Ipv4Addr::from(<&[u8; 4]>::try_from(&self.0[0..4]).unwrap().to_owned()),
                    u16::from_be_bytes(self.0[4..6].try_into().unwrap()),
                )),
            )
            .finish()
    }
}

impl Serialize for DhtPeerCompact {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_bytes(&self.0)
    }
}

impl<'de> Deserialize<'de> for DhtPeerCompact {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer
            .deserialize_bytes(BytesVisitor::<6>)
            .map(DhtPeerCompact)
    }
}

impl<'de: 'a, 'a> Deserialize<'de> for &'a DhtPeerCompact {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer
            .deserialize_bytes(BorrowedBytesVisitor::<6>)
            .map(|bytes| unsafe { &*(bytes.as_ptr() as *const _) })
    }
}

use bendy::serde as bencode;
pub fn to_bytes(query: KRpc) -> Result<Vec<u8>, bencode::Error> {
    bencode::to_bytes(&query)
}

pub fn from_bytes(buf: &[u8]) -> Result<KRpc<'_>, bencode::Error> {
    bencode::from_bytes::<'_, KRpc<'_>>(&buf)
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

    use crate::dht::{
        krpc::{DhtNodeCompactListOwned, DhtQuery, DhtResponse, GetPeersResult, KRpc, KRpcBody},
        DhtNode,
    };

    #[test]
    fn deserialize_ping_query() {
        let id = b"abcdefghij0123456789".into();
        let query = KRpc {
            transaction_id: b"aa",
            body: KRpcBody::Query(DhtQuery::Ping { id: &id }),
            version: None,
        };
        let bytes = super::to_bytes(query).unwrap();

        assert_eq!(
            bytes,
            "d1:ad2:id20:abcdefghij0123456789e1:q4:ping1:t2:aa1:y1:qe".as_bytes()
        );
    }

    #[test]
    fn deserialize_announce_peer_query() {
        let id = b"abcdefghij0123456789".into();
        let info_hash = b"mnopqrstuvwxyz123456".into();
        let query = KRpc {
            transaction_id: b"aa",
            body: KRpcBody::Query(DhtQuery::AnnouncePeer {
                id: &id,
                implied_port: Some(1),
                info_hash: &info_hash,
                port: 6881,
                token: b"aoeusnth",
            }),
            version: None,
        };
        let bytes = super::to_bytes(query).unwrap();

        assert_eq!(
            bytes,
            "d1:ad2:id20:abcdefghij012345678912:implied_porti1e9:info_hash20:mnopqrstuvwxyz1234564:porti6881e5:token8:aoeusnthe1:q13:announce_peer1:t2:aa1:y1:qe".as_bytes()
        );
    }

    #[test]
    fn deserialize_get_peers_response_found() {
        let p1 = b"012345".into();
        let p2 = b"a12345".into();
        let query = KRpc {
            transaction_id: b"aa",
            body: KRpcBody::Response {
                response: DhtResponse::GetPeers {
                    id: &b"abcdefghij0123456789".into(),
                    result: GetPeersResult::Found(vec![&p1, &p2]),
                    token: b"aoeusnth",
                },
            },
            version: None,
        };
        let bytes = super::to_bytes(query).unwrap();

        println!("{}", std::str::from_utf8(&bytes).unwrap());
        assert_eq!(
            bytes,
            "d1:rd2:id20:abcdefghij01234567895:token8:aoeusnth6:valuesl6:0123456:a12345ee1:t2:aa1:y1:re".as_bytes()
        );
    }

    #[test]
    fn deserialize_get_peers_response_not_found() {
        let node_list: DhtNodeCompactListOwned = vec![
            DhtNode::new(
                b"abcdefghij0123456789".into(),
                (
                    Ipv4Addr::new(b'1', b'1', b'1', b'1'),
                    u16::from_be_bytes(*b"ab"),
                )
                    .into(),
            ),
            DhtNode::new(
                b"xbcdefghij0123456789".into(),
                (
                    Ipv4Addr::new(b'2', b'2', b'2', b'2'),
                    u16::from_be_bytes(*b"cd"),
                )
                    .into(),
            ),
        ]
        .iter()
        .into();
        let query = KRpc {
            transaction_id: b"aa",
            body: KRpcBody::Response {
                response: DhtResponse::GetPeers {
                    id: &b"abcdefghij0123456789".into(),
                    result: GetPeersResult::NotFound((&node_list).into()),
                    token: b"aoeusnth",
                },
            },
            version: None,
        };
        let bytes = super::to_bytes(query).unwrap();

        println!("{}", std::str::from_utf8(&bytes).unwrap());
        assert_eq!(
            bytes,
            "d1:rd2:id20:abcdefghij01234567895:nodes52:abcdefghij01234567891111abxbcdefghij01234567892222cd5:token8:aoeusnthe1:t2:aa1:y1:re"
                .as_bytes()
        );
    }

    #[test]
    fn deserialize_get_peers_query() {
        let hex=b"64313a6164323a696432303ac398079e33dca64ef0eebb016c2ddb4d5289c37e393a696e666f5f6861736832303ad9d695ca1cf8c0b265998e1cb23cf1d5b527c7b565313a71393a6765745f7065657273313a74323a5ff3313a76343a4c54012e313a79313a7165";

        let bytes: Vec<_> = from_hex(hex);

        let packet = super::from_bytes(&bytes);
        assert!(matches!(
            packet.unwrap(),
            super::KRpc {
                body: KRpcBody::Query(super::DhtQuery::GetPeers { .. }),
                ..
            }
        ));
    }

    #[test]
    fn deserialize_get_peers_response() {
        let hex=b"64323a6970363a753cb72f1655313a7264323a696432303ada9ce84884e256606658fb8405b71ed68be4904b353a6e6f6465733230383adf46cb3d7b97fdad258f37b9ce8bf033d5d9146c6a69b26220fedc4c43a936178c467149c9a69c043254fbbd623425bb7d3fc8d5dea2623124c37d782f66450c72a9db8c89d0ddddde6972877e03ddbe3a8baf115aefdf52ad83b9865834f1f139aaa3ac0aa6c8d5dd93d5cb40950b670259657940a3d8ab536bacde7d8b752f1ddfdf473826c506516a4ba8841a7bc87bb0d547d0d7d5eef6e22711df9753279915cdc7e22d7df24db39ea3a27b31587cc22c451ae1de3be10579b969c2f62027e4fe99ca33a9834e232e9299a2bf69313a70693537313765353a746f6b656e343a27071c1665313a74323aee53313a76343a4c540102313a79313a7265";

        let bytes: Vec<_> = from_hex(hex);

        let packet = super::from_bytes(&bytes);
        assert!(matches!(
            packet.unwrap(),
            super::KRpc {
                body: KRpcBody::Response {
                    response: super::DhtResponse::GetPeers { .. }
                },
                ..
            }
        ));
    }
    #[test]
    fn deserialize_get_peers_response_found1() {
        let hex=b"64313a7264323a696432303a47ae7849b684c81f269c8519e4a1fd5f471cc340353a746f6b656e383a3837383136303235363a76616c7565736c363a7aee9fdb52e9363a1b9cde4121b6363a71f50b151bf3363a7260d20a26a5363a79cf29e362f7363a72609d1c26a5363a3a3ec4d225dd363adfd74d03595e363a1b9cde2e21b6363a3d8cc66b57d0363a74095aa3a552363a6e5ab1e321b6363a71f526d51bf36565313a74323a3424313a79313a7265";

        let bytes: Vec<_> = from_hex(hex);

        let packet = super::from_bytes(&bytes);
        match packet.unwrap() {
            super::KRpc {
                body:
                    KRpcBody::Response {
                        response:
                            super::DhtResponse::GetPeers {
                                result: GetPeersResult::Found(peers),
                                ..
                            },
                    },
                ..
            } => {
                for peer in &peers {
                    println!("{:?} = {:?}", peer, peer.0)
                }
                assert_eq!(peers[0].addr(), "122.238.159.219:21225".parse().unwrap());
                assert_eq!(peers[1].addr(), "27.156.222.65:8630".parse().unwrap());
                assert_eq!(peers[2].addr(), "113.245.11.21:7155".parse().unwrap());
                assert_eq!(peers[3].addr(), "114.96.210.10:9893".parse().unwrap());
                assert_eq!(peers[4].addr(), "121.207.41.227:25335".parse().unwrap());
                assert_eq!(peers[5].addr(), "114.96.157.28:9893".parse().unwrap());
                assert_eq!(peers[6].addr(), "58.62.196.210:9693".parse().unwrap());
                assert_eq!(peers[7].addr(), "223.215.77.3:22878".parse().unwrap());
            }
            _ => panic!("Not match"),
        }
    }

    #[test]
    fn deserialize_find_nodes_response() {
        let hex=b"64323a6970363a753cb72f1655313a7264323a696432303acd9dab3cfaa8ea3eff49c088905c46983722beee353a6e6f6465733230383acd9da1be1f0bb73a0b69da80f43fead0dd09331c46342dc84fd7cd9da24d71be8557a63ca35e86649daf3148aaad5f6fe177740bcd9da60a9c7220780ab09110e6bdd36bd593c69d5488cb591ae1cd9da3a59bc2ebfb0278c9b22cfd433fe7c957a0b5a8e9ea1ae1cd9da6e4b72a1a9a9bfc0acefaa3e9b37b33258bb4fb981bebe7cd9da60ee25cd6f349bc9699f245f930c366137372cd9ecca0f3cd9da08c00dfcd8c4e4594056ba6657a4b4c8201bc9a213dc8d5cd9dae5a263e4c5a0ba236280f166d013c0e456e725fd2c22181313a70693537313765353a746f6b656e343a45e621b365313a74323abf04313a76343a4c540101313a79313a7265";

        let bytes: Vec<_> = from_hex(hex);

        let packet = super::from_bytes(&bytes);
        assert!(matches!(
            packet.unwrap(),
            super::KRpc {
                body: KRpcBody::Response {
                    response: super::DhtResponse::GetPeers { .. }
                },
                ..
            }
        ));
    }
    #[test]
    fn deserialize_compact_peer() {
        let hex = b"363a7271abb192a2";

        let bytes: Vec<_> = from_hex(hex);

        let packet = bendy::serde::from_bytes::<super::DhtPeerCompact>(&bytes);
        assert_eq!(
            packet.unwrap().addr(),
            SocketAddr::V4(SocketAddrV4::new("114.113.171.177".parse().unwrap(), 37538))
        );
    }

    #[test]
    fn deserialize_announce_peer_query_1() {
        let hex=b"64313a6164323a696432303a832ab4a7493e16d272f9b40c3879d39784047db0393a696e666f5f6861736832303a4307ad94c16ff225cb8ce1a7bfa91e6b6d2f81b1343a706f7274693130373365353a746f6b656e353a406e8ae53d65313a7131333a616e6e6f756e63655f70656572313a74313a0d313a79313a7165";

        let bytes: Vec<_> = from_hex(hex);

        let packet = super::from_bytes(&bytes);
        assert!(matches!(
            packet.unwrap(),
            super::KRpc {
                body: KRpcBody::Query(super::DhtQuery::AnnouncePeer { .. }),
                ..
            }
        ));
    }

    #[test]
    fn deserialize_announce_peer_query_2() {
        let hex=b"64313a6164323a696432303a1ffb4da10d3087dee4239dd2daae8457a6cff4c731323a696d706c6965645f706f7274693065393a696e666f5f6861736832303a460c60b879c54d3032d8aef6f426a015c5d5adc3343a706f727469353331323165353a746f6b656e353a06fc54805c65313a7131333a616e6e6f756e63655f70656572313a74323a0a68313a79313a7165";

        let bytes: Vec<_> = from_hex(hex);

        let packet = super::from_bytes(&bytes);
        assert!(matches!(
            packet.unwrap(),
            super::KRpc {
                body: KRpcBody::Query(super::DhtQuery::AnnouncePeer { .. }),
                ..
            }
        ));
    }

    /// [BEP042](http://www.bittorrent.org/beps/bep_0042.html)
    ///
    /// In order to set ones initial node ID, the external IP needs to be known. This is not a trivial problem. With this extension, all DHT responses SHOULD include a top-level field called ip, containing a compact binary representation of the requestor's IP and port. That is big endian IP followed by 2 bytes of big endian port.
    #[test]
    fn deserialize_find_node_query_with_ip() {
        let hex=b"64323a6970363a72dc0eb11655313a7264323a696432303af4bca52bdda914127eec22750d651723135c94cb353a6e6f6465733230383a6f3aec5a118164562092bcebcb91fcb31607d9de5db1d3fff58d6e6a98bf2a6ed8f61d4e28dcebce28d9d0292f9d2591de729da16cb389332435457a1e1677ab7497a6428333b7a2dca80b581c406ce98e3c90ef6e0ffe2e54b25a8d21d0b0b7601872ecc01f1c256a472db6b32f24d75e56c11bb635b297ef9111b19a2dd8da0413681554d5984cd52e7039a606c71fd093a63290c9af8a2feb254c68f280d6ae529049f1f1bbe9ebb3a6db3c870ce1b9bd0e7e944d66531e5b4587b18f539eebabe288a9362e1eb5abba0c802ab5b2313a7069353731376565313a74323a1000313a76343a4c540101313a79313a7265";

        let bytes: Vec<_> = from_hex(hex);

        let packet = super::from_bytes(&bytes);
        assert!(matches!(
            packet.unwrap(),
            super::KRpc {
                body: KRpcBody::Response {
                    response: DhtResponse::FindNode { .. }
                },
                ..
            }
        ));
    }

    #[test]
    fn deserialize_find_node_query_unsorted() {
        let hex=b"64323a6970363a72dc0eb11655313a7264323a696432303a1c11e01be8e78d765a2e63339fc99a66320db754353a6e6f64657337383ae8680ab852123ec5e2ec87ab17f7b6163c6aaedbbc521f2ab97c0266f99287fcd1409d7143aa4a7380b7fab4b8a252c638431ae126297a7d49dcad4f14f2444066d06bc430b732f6d43a798ea86865313a74323a18c1313a79313a72313a76343a4c54010265";

        let bytes = from_hex(hex);

        let packet = super::from_bytes(&bytes);
        assert!(matches!(
            packet.unwrap(),
            super::KRpc {
                body: KRpcBody::Response {
                    response: DhtResponse::FindNode { .. }
                },
                ..
            }
        ));
    }

    #[test]
    fn deserialize_find_node_query_unsorted1() {
        let hex=b"64323a6970363a72dc0eb11655313a7264323a696432303a1c11e01be8e78d765a2e63339fc99a66320db754353a6e6f64657337383ae487337cab6926cb1a594c1c61b1db86cc23954f8623df5fdeab622f4d42ea87c1c11518ed85133bf953ddb896bb6a4ccad6d8e089c706a64eec2302093684c2fa829030237cccd96adcfad6532c65313a74323a5395313a79313a72313a76343a4c54010265";

        let bytes = from_hex(hex);

        let packet = super::from_bytes(&bytes);
        assert!(matches!(
            packet.unwrap(),
            super::KRpc {
                body: KRpcBody::Response {
                    response: DhtResponse::FindNode { .. }
                },
                ..
            }
        ));
    }

    fn from_hex(hex: &[u8]) -> Vec<u8> {
        let bytes: Vec<_> = hex
            .chunks_exact(2)
            .map(|src| u8::from_str_radix(std::str::from_utf8(src).unwrap(), 16).unwrap())
            .collect();
        bytes
    }
}
