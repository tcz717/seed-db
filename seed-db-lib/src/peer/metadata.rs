use std::{
    any::Any,
    fmt::Display,
    io::{self, ErrorKind},
    net::SocketAddr,
    sync::Arc,
};

use bitvec::prelude::*;
use bytes::{BufMut, Bytes, BytesMut};
use crypto::digest::Digest;
use futures::SinkExt;
use log::{info, warn};
use serde::{
    de::{Error, Unexpected, Visitor},
    ser::SerializeMap,
    Deserialize, Serialize,
};
use tokio::sync::Mutex;

use crate::dht::{DhtNodeId, InfoHash};

use super::{ExtensionPlugin, ExtensionPluginName, PeerConnection, LT_EXTENSION_MSG_ID};

const KB: u64 = 1024;
const PIECE_SIZE: usize = 16 * KB as usize;

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

#[derive(Debug)]
struct MetadataDownloadProgress {
    metadata: Box<[u8]>,
    pieces: BitVec,
}

impl MetadataDownloadProgress {
    fn new(size: usize) -> Self {
        Self {
            metadata: vec![0; size].into_boxed_slice(),
            pieces: bitvec![0; (size - 1) / PIECE_SIZE + 1],
        }
    }
}

pub async fn request_metadata_from_peer(
    addr: &SocketAddr,
    info_hash: &InfoHash,
    local_id: &DhtNodeId,
) -> Result<Box<[u8]>, std::io::Error> {
    let mut connection = PeerConnection::connect(&addr, &info_hash, &local_id).await?;

    connection.extension_handshake().await?;

    request_all_metadata(&mut connection).await
}

pub async fn request_all_metadata(
    connection: &mut PeerConnection,
) -> Result<Box<[u8]>, std::io::Error> {
    let remote_ext_id = connection
        .state
        .get_remote_ext_id(<MetadataExtension as ExtensionPluginName>::msg_name())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, MetadataError::NoPlugin))?;
    let local_ext_id = connection
        .state
        .get_local_ext_id(<MetadataExtension as ExtensionPluginName>::msg_name())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, MetadataError::NoPlugin))?;
    let progress = connection
        .state
        .get_extension::<MetadataExtension>()
        .and_then(|ext| ext.progress.as_ref())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, MetadataError::MissingSize))?
        .clone();
    while let Some(next_piece) = {
        let progress = progress.lock().await;
        progress
            .pieces
            .iter()
            .by_val()
            .enumerate()
            .find(|(_, p)| !*p)
            .map(|p| p.0)
    } {
        let request = MetadataMessage::Request { piece: next_piece };
        let mut buf = BytesMut::new();

        buf.put_u8(LT_EXTENSION_MSG_ID);
        buf.put_u8(remote_ext_id);
        buf.put(request.to_bytes().unwrap().as_slice());
        connection.stream.send(buf.freeze()).await?;

        while !progress.lock().await.pieces[next_piece] {
            connection.expect_extended_msg(local_ext_id).await?;
        }
    }

    let progress = progress.lock_owned().await;
    let mut hasher = crypto::sha1::Sha1::new();
    hasher.input(&progress.metadata);
    let mut hash = [0u8; 20];
    hasher.result(&mut hash);
    if &hash != connection.info_hash.as_ref().as_ref() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            MetadataError::HashNotMatch,
        ));
    }
    Ok::<Box<[u8]>, io::Error>(progress.metadata.clone())
}

#[derive(Debug, Default)]
pub struct MetadataExtension {
    progress: Option<Arc<Mutex<MetadataDownloadProgress>>>,
}

#[derive(Debug)]
pub enum MetadataError {
    TooLarge,
    Rejected,
    MissingSize,
    DeserializeFailed,
    InvalidDataSize,
    NotInit,
    NoPlugin,
    HashNotMatch,
}

impl std::error::Error for MetadataError {}

impl Display for MetadataError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&self, f)
    }
}

impl ExtensionPluginName for MetadataExtension {
    fn msg_name() -> &'static str {
        "ut_metadata"
    }
}

#[async_trait::async_trait]
impl ExtensionPlugin for MetadataExtension {
    async fn process(&mut self, data: &mut Bytes) -> Result<Option<Bytes>, std::io::Error> {
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
                    if let Some(ref mut metadata) = self.progress {
                        let mut progress = metadata.lock().await;
                        let start = piece * PIECE_SIZE;
                        let end = total_size.min(start + PIECE_SIZE);
                        let piece_size = end - start;

                        if end > progress.metadata.len() {
                            return Err(io::Error::new(
                                ErrorKind::InvalidData,
                                MetadataError::InvalidDataSize,
                            ));
                        }

                        let data = data.split_off(data.len() - piece_size);
                        progress.metadata[start..end].copy_from_slice(&data);
                        *progress.pieces.get_mut(piece).unwrap() = true;

                        info!("Piece {} ({}..{}) downloaded", piece, start, end);

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
        if let Some(size) = handshake.metadata_size {
            if size > 512 * KB {
                warn!("Metadata size {} too large", size);
                return Err(io::Error::new(
                    ErrorKind::InvalidData,
                    MetadataError::TooLarge,
                ));
            }
            if self.progress.is_none() {
                info!("Metadata size: {}", size);
                self.progress = Some(Arc::new(Mutex::new(MetadataDownloadProgress::new(
                    size as usize,
                ))));
            }
        } else {
            warn!("Metadata size not specified in handshake")
        }
        Ok(())
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn as_any(&self) -> &dyn Any {
        self
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
