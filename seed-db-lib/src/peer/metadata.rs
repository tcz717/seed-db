use std::{
    fmt::Display,
    io::{self, ErrorKind},
};

use bitvec::prelude::*;
use bytes::Bytes;
use log::warn;
use serde::{
    de::{Error, Unexpected, Visitor},
    ser::SerializeMap,
    Deserialize, Serialize,
};

use super::ExtensionPlugin;

const KB: u64 = 1024;

#[derive(Debug)]
pub enum MetadataMessage {
    Request { piece: usize },
    Data { piece: usize, total_size: usize },
    Reject,
}

impl MetadataMessage {
    pub fn from_bytes(data: &mut Bytes) -> Option<Self> {
        bendy::serde::from_bytes(&data).ok()
    }

    pub fn to_bytes(&self) -> Option<Vec<u8>> {
        bendy::serde::to_bytes(self).ok()
    }
}

impl<'de> Deserialize<'de> for MetadataMessage {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Default)]
        struct MetadataMessageVisitor {
            msg_type: Option<usize>,
            piece: Option<usize>,
            total_size: Option<usize>,
        }

        impl<'de> Visitor<'de> for MetadataMessageVisitor {
            type Value = MetadataMessage;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("Expecting MetadataMessage")
            }

            fn visit_map<A>(mut self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::MapAccess<'de>,
            {
                while let Some((key, value)) = map.next_entry::<&'de str, usize>()? {
                    match key {
                        "msg_type" => self.msg_type = Some(value),
                        "piece" => self.piece = Some(value),
                        "total_size" => self.total_size = Some(value),
                        unknown => {
                            return Err(Error::unknown_field(
                                unknown,
                                &["msg_type", "piece", "total_size"],
                            ))
                        }
                    }
                }
                match self.msg_type {
                    Some(0) => Ok(MetadataMessage::Request {
                        piece: self.piece.ok_or(Error::missing_field("piece"))?,
                    }),
                    Some(1) => Ok(MetadataMessage::Data {
                        piece: self.piece.ok_or(Error::missing_field("piece"))?,
                        total_size: self.total_size.ok_or(Error::missing_field("total_size"))?,
                    }),
                    Some(3) => Ok(MetadataMessage::Reject),
                    Some(unknown) => Err(Error::invalid_value(
                        Unexpected::Unsigned(unknown as u64),
                        &"0 to 2",
                    )),
                    None => Err(Error::missing_field("msg_type")),
                }
            }
        }

        deserializer.deserialize_struct("", &[], MetadataMessageVisitor::default())
    }
}

impl Serialize for MetadataMessage {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut ser = serializer.serialize_map(None)?;
        match self {
            MetadataMessage::Request { piece } => {
                ser.serialize_entry("msg_type", &0)?;
                ser.serialize_entry("piece", piece)?;
            }
            MetadataMessage::Data { piece, total_size } => {
                ser.serialize_entry("msg_type", &1)?;
                ser.serialize_entry("piece", piece)?;
                ser.serialize_entry("total_size", total_size)?;
            }
            MetadataMessage::Reject => {
                ser.serialize_entry("msg_type", &3)?;
            }
        }

        ser.end()
    }
}

#[derive(Debug, Default)]
pub struct MetadataExtension {
    metadata: Option<Box<[u8]>>,
    pieces: BitVec,
}

#[derive(Debug)]
pub enum MetadataError {
    TooLarge,
    Rejected,
    MissingSize,
    DeserializeFailed,
    InvalidDataSize,
}

impl std::error::Error for MetadataError {}

impl Display for MetadataError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&self, f)
    }
}

impl ExtensionPlugin for MetadataExtension {
    fn process(&mut self, data: &mut Bytes) -> Result<Option<Bytes>, std::io::Error> {
        if let Some(msg) = MetadataMessage::from_bytes(data) {
            match msg {
                MetadataMessage::Request { .. } => {
                    let reply = MetadataMessage::Reject.to_bytes();
                    if let Some(reply) = reply {
                        Ok(Some(reply.into()))
                    } else {
                        Err(io::Error::new(
                            ErrorKind::InvalidData,
                            MetadataError::DeserializeFailed,
                        ))
                    }
                }
                MetadataMessage::Data { piece, total_size } => {
                    if let Some(ref mut metadata) = self.metadata {
                        let start = piece * (16 * KB as usize);
                        let end = start + total_size;

                        if end > metadata.len() {
                            return Err(io::Error::new(
                                ErrorKind::InvalidData,
                                MetadataError::InvalidDataSize,
                            ));
                        }

                        let data = data.split_off(data.len() - total_size);
                        metadata[start..end].copy_from_slice(&data);
                        *self.pieces.get_mut(piece).unwrap() = false;

                        Ok(None)
                    } else {
                        Err(io::Error::new(
                            ErrorKind::InvalidData,
                            MetadataError::MissingSize,
                        ))
                    }
                }
                MetadataMessage::Reject => Err(io::Error::new(
                    ErrorKind::ConnectionRefused,
                    MetadataError::Rejected,
                )),
            }
        } else {
            Ok(None)
        }
    }

    fn msg_name(&self) -> &'static str {
        "ut_metadata"
    }

    fn handshake(&mut self, handshake: &super::ExtensionHandshake) -> Result<(), io::Error> {
        if let Some(size) = handshake.matadata_size {
            if size > 512 * KB {
                warn!("Metadata size {} too large", size);
                return Err(io::Error::new(
                    ErrorKind::InvalidData,
                    MetadataError::TooLarge,
                ));
            }
            if self.metadata.is_none() {
                self.metadata = Some(vec![0; size as usize].into_boxed_slice());
                self.pieces.resize(size as usize, false);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::MetadataMessage;

    #[test]
    fn deserialize_metadata_msg_data() {
        let raw = b"d8:msg_typei1e5:piecei0e10:total_sizei8eexxxxxxxx";
        let msg: MetadataMessage = bendy::serde::from_bytes(raw).unwrap();
        assert!(matches!(msg, MetadataMessage::Data { .. }));

        match msg {
            MetadataMessage::Data { piece, total_size } => {
                assert_eq!(piece, 0);
                assert_eq!(total_size, 8);

                assert_eq!(&raw[raw.len() - total_size..], b"xxxxxxxx");
            }
            _ => unreachable!(),
        }
    }
}
