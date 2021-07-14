use super::utils::{BytesSliceVisitor, BytesVisitor};
use super::{DhtNode, DhtNodeId, InfoHash};
use serde::{Deserialize, Serialize, Serializer};
use std::convert::{TryFrom, TryInto};
use std::fmt::Debug;
use std::mem::size_of;
use std::net::{Ipv4Addr, SocketAddr};

#[derive(Serialize, Deserialize, PartialEq, Debug)]
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

#[derive(Serialize, Deserialize, PartialEq, Debug)]
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
            id: DhtNodeId::new(self.0[0..20].try_into().unwrap()),
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

use bendy::serde as bencode;
pub fn to_bytes(query: KRpc) -> Result<Vec<u8>, bencode::Error> {
    bencode::to_bytes(&query)
}

pub fn from_bytes(buf: &[u8]) -> Result<KRpc<'_>, bencode::Error> {
    bencode::from_bytes::<'_, KRpc<'_>>(&buf)
}

#[cfg(test)]
mod tests {
    use crate::dht::krpc::{DhtQuery, DhtResponse, GetPeersResult, KRpc, KRpcBody};

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
                implied_port: Some(true),
                info_hash: &info_hash,
                port: 6881,
                token: "aoeusnth",
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
    fn deserialize_get_peers_response() {
        let query = KRpc {
            transaction_id: b"aa",
            body: KRpcBody::Response {
                response: DhtResponse::GetPeers {
                    id: &b"abcdefghij0123456789".into(),
                    result: GetPeersResult::Found(vec![
                        b"01234567890123456789".into(),
                        b"a1234567890123456789".into(),
                    ]),
                    token: b"aoeusnth",
                },
            },
            version: None,
        };
        let bytes = super::to_bytes(query).unwrap();

        println!("{}", std::str::from_utf8(&bytes).unwrap());
        assert_eq!(
            bytes,
            "d1:rd2:id20:abcdefghij01234567895:token8:aoeusnth6:valuesl20:0123456789012345678920:a1234567890123456789ee1:t2:aa1:y1:re".as_bytes()
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
