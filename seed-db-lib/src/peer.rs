use std::{
    any::Any, collections::HashMap, fmt::Display, io, mem::size_of, net::SocketAddr, ops::Deref,
    sync::Arc, time::Duration,
};

use bitflags::bitflags;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use futures::{
    stream::{SplitSink, SplitStream},
    SinkExt, StreamExt, TryStreamExt,
};
use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    spawn,
    sync::{Mutex, Notify},
    time::timeout,
};
use tokio_util::codec::{length_delimited, Framed, LengthDelimitedCodec};

use crate::dht::{DhtNodeId, InfoHash};

pub type PeerId = DhtNodeId;

pub mod metadata;

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

const LT_EXTENSION_MSG_ID: u8 = 20;
const HANDSHAKE_EXTENDED_MSG_ID: u8 = 0;

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
    matadata_size: Option<u64>,
}

pub type ExtensionMessages = HashMap<String, u8>;

#[derive(Debug)]
pub struct PeerConnectionState {
    local_interest: bool,
    local_chock: bool,
    remote_interest: bool,
    remote_chock: bool,
    local_extension: ExtensionMessages,
    remote_extension: Option<ExtensionMessages>,
    handlers: Vec<Box<dyn ExtensionPlugin + 'static>>,
}

impl PeerConnectionState {
    pub fn get_extension<Ext: ExtensionPluginName + 'static>(&self) -> Option<&Ext> {
        // TODO find by name?
        self.handlers
            .iter()
            .find_map(|ext| ext.as_any().downcast_ref())
    }
    pub fn get_extension_mut<Ext: ExtensionPluginName + 'static>(&mut self) -> Option<&mut Ext> {
        // TODO find by name?
        self.handlers
            .iter_mut()
            .find_map(|ext| ext.as_any_mut().downcast_mut())
    }

    pub fn get_remote_ext_id(&self, name: &str) -> Option<u8> {
        self.remote_extension
            .as_ref()
            .and_then(|exts| exts.get(name).copied())
    }

    pub fn lookup_local_msg_name(&self, extended_id: u8) -> Option<&String> {
        self.local_extension
            .iter()
            .find(|(_, ext_id)| **ext_id == extended_id)
            .map(|p| p.0)
    }
}

impl Default for PeerConnectionState {
    fn default() -> Self {
        Self {
            local_interest: false,
            local_chock: true,
            remote_interest: false,
            remote_chock: true,
            local_extension: Default::default(),
            remote_extension: Default::default(),
            handlers: Default::default(),
        }
    }
}

#[async_trait::async_trait]
pub trait ExtensionPlugin: std::fmt::Debug + Send {
    fn resister(&mut self, _connection: &mut PeerConnection) {}
    async fn process(&mut self, msg: &mut Bytes) -> Result<Option<Bytes>, io::Error>;
    fn handshake(&mut self, handshake: &ExtensionHandshake) -> Result<(), io::Error>;
    fn msg_name(&self) -> &'static str;
    fn as_any_mut(&mut self) -> &mut dyn Any;
    fn as_any(&self) -> &dyn Any;
}

pub trait ExtensionPluginName: ExtensionPlugin {
    fn msg_name() -> &'static str;
}

pub type PeerSink = SplitSink<Framed<TcpStream, LengthDelimitedCodec>, Bytes>;

pub type SharedConnectionState = Arc<Mutex<PeerConnectionState>>;

pub struct PeerConnection {
    sink: Arc<Mutex<PeerSink>>,
    info_hash: Box<InfoHash>,
    local_peer_id: Box<PeerId>,
    remote_peer_id: Box<PeerId>,
    reserved_bits: PeerReservedBit,
    state: SharedConnectionState,
    state_changed: Arc<Notify>,
}

impl PeerConnection {
    pub async fn connect(
        addr: &SocketAddr,
        info_hash: &InfoHash,
        local_peer_id: &PeerId,
    ) -> Result<PeerConnection, io::Error> {
        let mut stream = TcpStream::connect(addr).await?;
        stream.write_u8(19).await?;
        stream.write_all(BIT_TORRENT_PROTOCOL).await?;
        stream.write_all(&[0u8; 8]).await?;
        stream.write_all(info_hash.as_ref()).await?;

        println!("Sent handshake");

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

        println!("Handshake done");

        let (sink, stream) = length_delimited::Builder::new()
            .length_field_length(4)
            .big_endian()
            .new_framed(stream)
            .split();
        let sink = Arc::new(Mutex::new(sink));
        let state: Arc<Mutex<PeerConnectionState>> = Default::default();
        let state_changed: Arc<Notify> = Default::default();
        state
            .lock()
            .await
            .handlers
            .push(Box::new(metadata::MetadataExtension::default()));

        Self::start_listen_thread(stream, sink.clone(), state.clone(), state_changed.clone());

        Ok(Self {
            sink,
            info_hash: Box::new(info_hash.clone()),
            local_peer_id: Box::new(local_peer_id.clone()),
            remote_peer_id: Box::new(remote_peer_id.clone()),
            reserved_bits: PeerReservedBit::from_bits_truncate(u64::from_be_bytes(reserved_bytes))
                & PeerReservedBit::SUPPORTED,
            state,
            state_changed,
        })
    }

    fn start_listen_thread(
        mut stream: SplitStream<Framed<TcpStream, LengthDelimitedCodec>>,
        sink: Arc<Mutex<PeerSink>>,
        state: Arc<Mutex<PeerConnectionState>>,
        state_changed: Arc<Notify>,
    ) {
        spawn(async move {
            while let Some(mut data) = stream.try_next().await? {
                let msg_id = data.get_u8();
                match msg_id {
                    LT_EXTENSION_MSG_ID => {
                        Self::handle_extension_msg(data.freeze(), &sink, &state, &state_changed)
                            .await?;
                    }
                    _ => (),
                }
            }
            Ok::<(), io::Error>(())
        });
    }

    async fn handle_extension_msg(
        mut data: Bytes,
        sink: &Arc<Mutex<PeerSink>>,
        state: &Arc<Mutex<PeerConnectionState>>,
        state_changed: &Arc<Notify>,
    ) -> Result<(), std::io::Error> {
        let extended_id = data.get_u8();
        if extended_id == HANDSHAKE_EXTENDED_MSG_ID {
            let handshake: ExtensionHandshake = bendy::serde::from_bytes(&data).unwrap();

            let mut state = state.lock().await;

            state.remote_extension = Some(handshake.messages);
            state_changed.notify_one();
        } else {
            let mut state = state.lock().await;
            let msg_name = state.lookup_local_msg_name(extended_id).cloned();
            let remote_ext_id = msg_name
                .as_ref()
                .and_then(|name| state.get_remote_ext_id(name));
            let handler = msg_name.and_then(|msg_name| {
                state
                    .handlers
                    .iter_mut()
                    .find(|ext| ext.msg_name() == msg_name)
            });
            if let (Some(handler), Some(remote_ext_id)) = (handler, remote_ext_id) {
                let reply = handler.process(&mut data).await?;
                if let Some(reply) = reply {
                    let mut buf = BytesMut::with_capacity(reply.len() + 2 * size_of::<u8>());
                    buf.put_u8(LT_EXTENSION_MSG_ID);
                    buf.put_u8(remote_ext_id);
                    buf.put(reply);
                    sink.lock().await.send(buf.freeze()).await?;
                }
            }
        }
        Ok(())
    }

    pub async fn extension_handshake(&mut self) -> Result<bool, io::Error> {
        if !self
            .reserved_bits
            .contains(PeerReservedBit::LIBTORRENT_EXTENSION_PROTOCOL)
        {
            return Ok(false);
        }

        let mut buf = BytesMut::with_capacity(32);
        buf.put_u8(LT_EXTENSION_MSG_ID);
        buf.put_u8(HANDSHAKE_EXTENDED_MSG_ID);
        let handshake = ExtensionHandshake {
            messages: self
                .state
                .lock()
                .await
                .handlers
                .iter()
                .enumerate()
                .map(|(id, handler)| (handler.msg_name().to_owned(), id as u8))
                .collect(),
            matadata_size: None,
        };

        buf.put(bendy::serde::to_bytes(&handshake).unwrap().as_ref());
        self.sink.lock().await.send(buf.freeze()).await?;
        self.state.lock().await.local_extension = handshake.messages;

        for _ in 0..5 {
            if let Ok(_) = timeout(Duration::from_secs(1), self.state_changed.notified()).await {
                if self.state.lock().await.remote_extension.is_some() {
                    return Ok(true);
                }
            }
        }

        Ok(false)
    }

    /// Get a reference to the peer connection's remote peer id.
    pub fn remote_peer_id(&self) -> &PeerId {
        self.remote_peer_id.as_ref()
    }

    /// Get a reference to the peer connection's local peer id.
    pub fn local_peer_id(&self) -> &PeerId {
        self.local_peer_id.as_ref()
    }

    /// Get a reference to the peer connection's info hash.
    pub fn info_hash(&self) -> &InfoHash {
        self.info_hash.as_ref()
    }
}

impl Deref for PeerConnection {
    type Target = Arc<Mutex<PeerConnectionState>>;

    fn deref(&self) -> &Self::Target {
        &self.state
    }
}

#[cfg(test)]
mod tests {
    use std::{convert::TryInto, net::SocketAddr, str::FromStr};

    use crate::{
        dht::InfoHash,
        peer::{metadata::MetadataExtension, PeerConnection},
    };

    #[tokio::test]
    async fn get_metadata() {
        let addr = SocketAddr::from_str("45.147.196.162:48724").unwrap();
        let hash = base64::decode("pdOV1q5SkEnx8bvp67Om2zyHDOE=").unwrap();

        assert_eq!(hash.len(), 20);
        let hash = InfoHash::new(hash.as_slice().try_into().unwrap());
        let local_id = InfoHash::random();

        let mut connection = PeerConnection::connect(&addr, &hash, &local_id)
            .await
            .unwrap();

        assert_eq!(connection.extension_handshake().await.unwrap(), true);

        let res = {
            let state = connection.lock().await;
            let handler = state.get_extension::<MetadataExtension>().unwrap();
            handler.request_all_metadata(&state)
        }
        .unwrap()
        .run()
        .await
        .ok()
        .flatten()
        .unwrap();

        assert!(res.len() > 0)
    }
}
