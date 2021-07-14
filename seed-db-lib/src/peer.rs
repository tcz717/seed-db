use std::{fmt::Display, io, net::SocketAddr};

use bitflags::bitflags;
use bytes::{BufMut, BytesMut};
use futures::SinkExt;
use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};
use tokio_util::codec::{length_delimited, Framed, LengthDelimitedCodec};

use crate::dht::{DhtNodeId, InfoHash};

pub type PeerId = DhtNodeId;

#[derive(Debug)]
pub enum PeerProtocalError {
    InvaildHead,
    InfoHashNotMatched,
}

impl std::error::Error for PeerProtocalError {}

impl Display for PeerProtocalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PeerProtocalError::InvaildHead => f.write_str("Invalid head"),
            PeerProtocalError::InfoHashNotMatched => f.write_str("Info hash not matched"),
        }
    }
}

const BIT_TORRENT_PROTOCOL: &[u8; 19] = b"BitTorrent protocol";

bitflags! {
    /// ```txt
    /// reserved[0]
    /// 0x80  Azureus Messaging Protocol
    ///
    /// reserved[2]
    /// 0x08  BitTorrent Location-aware Protocol (no known implementations)
    ///
    /// reserved[5]
    /// 0x10  LTEP (Libtorrent Extension Protocol)
    /// 0x02  Extension Negotiation Protocol
    /// 0x01  Extension Negotiation Protocol
    ///
    /// reserved[7]
    /// 0x01  BitTorrent DHT
    /// 0x02  XBT Peer Exchange
    /// 0x04  suggest, haveall, havenone, reject request, and allow fast extensions
    /// 0x08  NAT Traversal
    /// 0x10  hybrid torrent legacy to v2 upgrade
    /// ```
    pub struct PeerReservedBit: u64 {
        const AZUREUS_MESSAGING_PROTOCOL = 0x80_00_00_00_00_00_00_00;
        const BIT_TORRENT_LOCATION_AWARE_PROTOCOL = 0x00_00_08_00_00_00_00_00;
        const LIBTORRENT_EXTENSION_PROTOCOL = 0x00_00_00_00_00_10_00_00;
        const EXTENSION_NEGOTIATION_PROTOCOL2 = 0x00_00_00_00_00_02_00_00;
        const EXTENSION_NEGOTIATION_PROTOCOL1 = 0x00_00_00_00_00_01_00_00;
        const BIT_TORRENT_DHT = 0x00_00_00_00_00_00_00_01;
        const XBT_PEER_EXCHANGE = 0x00_00_00_00_00_00_00_02;
        const FAST_EXTENSIONS = 0x00_00_00_00_00_00_00_04_;
        const NAT_TRAVERSAL = 0x00_00_00_00_00_00_00_08;
        const HYBRID_TORRENT_V2_UPGRADE = 0x00_00_00_00_00_00_00_10;

        const SUPPORTED = Self::LIBTORRENT_EXTENSION_PROTOCOL.bits;
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ExtensionHandshake {
    #[serde(rename = "m")]
    messages: ExtensionMessages,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ExtensionMessages {
    #[serde(default)]
    #[serde(rename = "ut_metadata")]
    metadata: u8,
}

pub struct PeerConnection {
    stream: Framed<TcpStream, LengthDelimitedCodec>,
    info_hash: Box<InfoHash>,
    local_peer_id: Box<PeerId>,
    remote_peer_id: Box<PeerId>,
    reserved_bits: PeerReservedBit,
}

impl PeerConnection {
    pub async fn connect(
        addr: &SocketAddr,
        info_hash: &InfoHash,
        local_peer_id: &PeerId,
        reserved: PeerReservedBit,
    ) -> Result<PeerConnection, io::Error> {
        let mut stream = TcpStream::connect(addr).await?;
        stream.write_u8(19).await?;
        stream.write_all(BIT_TORRENT_PROTOCOL).await?;
        stream.write_all(&[0u8; 8]).await?;
        stream.write_all(info_hash.as_ref()).await?;

        if stream.read_u8().await? != 19 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                PeerProtocalError::InvaildHead,
            ));
        }
        if &{
            let mut buf = [0u8; 19];
            stream.read_exact(&mut buf).await?;
            buf
        } != BIT_TORRENT_PROTOCOL
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                PeerProtocalError::InvaildHead,
            ));
        }
        let mut reserved_bytes = [0u8; 8];
        stream.read_exact(&mut reserved_bytes).await?;

        if &{
            let mut buf = [0u8; 20];
            stream.read_exact(&mut buf).await?;
            buf
        } != info_hash.as_ref()
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                PeerProtocalError::InfoHashNotMatched,
            ));
        }

        stream.write_all(local_peer_id.as_ref()).await?;

        let mut remote_peer_id = [0u8; 20];
        stream.read_exact(&mut remote_peer_id).await?;
        let remote_peer_id = PeerId::new(&remote_peer_id);

        Ok(Self {
            stream: length_delimited::Builder::new()
                .length_field_length(4)
                .big_endian()
                .new_framed(stream),
            info_hash: Box::new(info_hash.clone()),
            local_peer_id: Box::new(local_peer_id.clone()),
            remote_peer_id: Box::new(remote_peer_id.clone()),
            reserved_bits: PeerReservedBit::from_bits_truncate(u64::from_be_bytes(reserved_bytes))
                & reserved,
        })
    }

    pub async fn extension_handshake(&mut self) -> Result<(), io::Error> {
        let mut buf = BytesMut::with_capacity(32);
        buf.put_u8(20);
        let handshake = ExtensionHandshake {
            messages: ExtensionMessages { metadata: 1 },
        };

        buf.put(bendy::serde::to_bytes(&handshake).unwrap().as_ref());
        self.stream.send(buf.freeze()).await?;
        Ok(())
    }
}
