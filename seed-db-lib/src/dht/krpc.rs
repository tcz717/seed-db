use super::utils::{BytesSliceVisitor, BytesVisitor};
use super::{DhtNode, DhtNodeId, InfoHash};
use serde::{Deserialize, Serialize, Serializer};
use std::convert::{TryFrom, TryInto};
use std::fmt::Debug;
use std::mem::size_of;
use std::net::{Ipv4Addr, SocketAddr};

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

impl<'a> KRpc<'a> {
    pub fn node_id(&self) -> Option<&DhtNodeId> {
        match self {
            KRpc::Query { query, .. } => match query {
                DhtQuery::AnnouncePeer { id, .. } => Some(*id),
                DhtQuery::FindNode { id, .. } => Some(*id),
                DhtQuery::GetPeers { id, .. } => Some(*id),
                DhtQuery::Ping { id } => Some(*id),
            },
            KRpc::Response { response, .. } => match response {
                DhtResponse::FindNode { id, .. } => Some(*id),
                DhtResponse::GetPeers { id, .. } => Some(*id),
                DhtResponse::PingOrAnnouncePeer { id } => Some(*id),
            },
            KRpc::Error { .. } => None,
        }
    }
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
        info_hash: &'a InfoHash,
        port: u16,
        token: &'a str,
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
#[derive(Serialize, Deserialize, PartialEq, Debug)]
#[serde(untagged)]
pub enum DhtResponse<'a> {
    FindNode {
        id: &'a DhtNodeId,
        nodes: DhtNodeCompactList<'a>,
    },
    GetPeers {
        id: &'a DhtNodeId,
        #[serde(with = "serde_bytes")]
        token: &'a [u8],
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
            data.extend_from_slice(node.id().as_ref())
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

pub fn to_bytes(query: KRpc) -> Result<Vec<u8>, bendy::serde::Error> {
    bendy::serde::to_bytes(&query)
}

pub fn from_bytes(buf: &[u8; 1024], len: usize) -> Result<KRpc<'_>, bendy::serde::Error> {
    bendy::serde::from_bytes::<'_, KRpc<'_>>(&buf[..len])
}

#[cfg(test)]
mod tests {
    use crate::dht::krpc::{DhtQuery, DhtResponse, GetPeersResult, KRpc};

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
                info_hash: &b"mnopqrstuvwxyz123456".into(),
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
                token: b"aoeusnth",
            },
        };
        let bytes = bendy::serde::to_bytes(&query).unwrap();

        println!("{}", std::str::from_utf8(&bytes).unwrap());
        assert_eq!(
            bytes,
            "d1:rd2:id20:abcdefghij01234567895:token8:aoeusnth6:valuesl20:0123456789012345678920:a1234567890123456789ee1:t2:aa1:y1:re".as_bytes()
        );
    }
}
