use std::{net::SocketAddr, sync::Arc, time::Duration};

use crate::{
    dht::{DhtClientController, DhtNodeId, InfoHash},
    peer::metadata::request_metadata_from_peer,
};
use bendy::{decoding::FromBencode, value::Value};
use futures::StreamExt;
use hashbrown::HashSet;
use itertools::Itertools;
use log::{error, info, trace, warn};
use lru_time_cache::LruCache;
use tokio::{pin, sync::Mutex, time::timeout};

pub struct SeedCrawler {
    dht_client: DhtClientController,
}

impl SeedCrawler {
    pub fn new(dht_client: DhtClientController) -> Self {
        Self { dht_client }
    }

    pub async fn crawl(&mut self) {
        let blacklist = Arc::new(Mutex::new(LruCache::with_capacity(128)));
        let visited = Arc::new(Mutex::new(HashSet::new()));
        let local_peer_id = self.dht_client.id();
        let sender = self.dht_client.sender();

        let metadatas = self
            .dht_client
            .info_hash_observer()
            .map(|observation| {
                let blacklist = blacklist.clone();
                let visited = visited.clone();
                let local_peer_id = local_peer_id.clone();
                let sender = sender.clone();
                async move {
                    let (info_hash, peer_addrs) = match observation {
                        crate::dht::InfoHashObservation::WithPeerAddr {
                            info_hash,
                            peer_addr,
                        } => (info_hash, vec![peer_addr]),
                        crate::dht::InfoHashObservation::WithUtpPeerAddr { .. } => {
                            warn!("Got Utp peer, not supported yet");
                            return None;
                        }
                        crate::dht::InfoHashObservation::WithDhtNode { info_hash, node } => {
                            return None;
                            if !visited.lock().await.insert(info_hash.clone()) {
                                return None;
                            }
                            let peers = sender
                                .lock()
                                .await
                                .lookup_peers(&info_hash, Some(&node))
                                .await;
                            if let Err(err) = peers {
                                trace!("{}", err);
                                return None;
                            }
                            info!("Found peers for hash {}: {:?}", info_hash, peers);
                            (info_hash, peers.unwrap())
                        }
                    };

                    let addrs: Vec<SocketAddr> = {
                        let mut blacklist = blacklist.lock().await;
                        peer_addrs
                            .iter()
                            .filter(|addr| {
                                blacklist.get(&addr.ip()).map(|i| *i < 10).unwrap_or(true)
                            })
                            .cloned()
                            .collect()
                    };
                    if addrs.is_empty() {
                        info!("Peers of {:?} are all in blacklist, droped", info_hash);
                        return None;
                    }
                    info!("Start downloading metadata for {:?}", info_hash);
                    let result = download_metadata(addrs, local_peer_id, info_hash).await;

                    if result.is_none() {
                        for addr in peer_addrs {
                            let mut blacklist = blacklist.lock().await;
                            let failed_count = blacklist.entry(addr.ip()).or_insert(0);
                            *failed_count += 1;
                            info!("{} failed {} times", addr.ip(), failed_count)
                        }
                    }
                    result
                }
            })
            .buffer_unordered(8)
            .filter_map(|metadata| async { metadata });
        pin!(metadatas);
        while let Some(observation) = metadatas.next().await {
            let metadata = Value::from_bencode(&observation);
            let name_key = std::borrow::Cow::Borrowed(&b"name"[..]);
            match metadata {
                Ok(metadata) => {
                    info!(
                        "Got seed: {:?}",
                        if let Value::Dict(ref dict) = metadata {
                            dict.get(&name_key)
                        } else {
                            None
                        }
                    )
                }
                Err(err) => error!("Metadata decode failed, {}", err),
            }
        }
    }
}

async fn download_metadata(
    peer_addr: Vec<SocketAddr>,
    local_peer_id: Arc<DhtNodeId>,
    info_hash: Arc<InfoHash>,
) -> Option<Box<[u8]>> {
    let metadata = futures::stream::iter(peer_addr.iter().unique())
        .map(|addr| {
            let local_peer_id = local_peer_id.clone();
            let info_hash = info_hash.clone();
            async move {
                info!("Connecting peer {}", addr);
                let result = timeout(
                    Duration::from_secs(10),
                    request_metadata_from_peer(&addr, &info_hash, &local_peer_id),
                )
                .await;
                match result {
                    Err(_) => {
                        error!("Download metadata from {} timeout", addr);
                        None
                    }
                    Ok(result) => {
                        if let Err(err) = &result {
                            error!("Failed to download metadata from {}: {}", addr, err);
                            return None;
                        }
                        result.ok()
                    }
                }
            }
        })
        .buffer_unordered(8)
        .filter_map(|metadata| async { metadata });
    pin!(metadata);
    metadata.next().await
}
