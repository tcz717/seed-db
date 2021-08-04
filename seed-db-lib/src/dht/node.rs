use hex::FromHex;
use serde::de::Visitor;
use serde::{Deserialize, Serialize, Serializer};

use std::borrow::Borrow;
use std::convert::{TryFrom, TryInto};
use std::fmt::{Debug, Display};
use std::net::SocketAddr;
use std::sync::Arc;

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
            if f.alternate() {
                write!(f, "{:02X} ", i)?
            } else {
                write!(f, "{:02X}", i)?
            }
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

impl Borrow<[u8; 20]> for DhtNodeId {
    fn borrow(&self) -> &[u8; 20] {
        &self.0
    }
}

impl Into<DhtNodeId> for &[u8; 20] {
    fn into(self) -> DhtNodeId {
        DhtNodeId::new(self)
    }
}

impl FromHex for DhtNodeId {
    type Error = ();

    fn from_hex<T: AsRef<[u8]>>(hex: T) -> Result<Self, Self::Error> {
        Ok(Self(
            Vec::<u8>::from_hex(hex)
                .map_err(|_| ())?
                .try_into()
                .map_err(|_| ())?,
        ))
    }
}

impl TryFrom<&[u8]> for DhtNodeId {
    type Error = std::array::TryFromSliceError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        Ok(Self(value.try_into()?))
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

#[derive(Debug, PartialEq, Eq, Hash, Clone, Ord, PartialOrd)]
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

    pub fn distence_order(&self, target: &DhtNodeId) -> OrderedNode {
        OrderedNode {
            node: self.clone(),
            idx: &self.id ^ target,
        }
    }
}

impl Display for DhtNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Node<{}, {}>", self.id, self.addr)
    }
}

#[derive(PartialEq, Eq, Debug)]
pub struct OrderedNode {
    node: DhtNode,
    idx: DhtNodeId,
}

impl OrderedNode {
    /// Get a reference to the ordered node's node.
    pub fn node(&self) -> &DhtNode {
        &self.node
    }
}

impl Ord for OrderedNode {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.idx.cmp(&other.idx)
    }
}
impl PartialOrd for OrderedNode {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.idx.partial_cmp(&other.idx)
    }
}

pub type SharedDhtNode = Arc<DhtNode>;

#[cfg(test)]
mod tests {
    use super::DhtNodeId;

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
