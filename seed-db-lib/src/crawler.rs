use std::{error::Error, sync::Arc, time::Duration};

use crate::{dht::{DhtClientController, DhtNodeId, InfoHash}, peer::metadata::request_metadata_from_peer};
use bendy::decoding::FromBencode;
use futures::StreamExt;
use hashbrown::HashSet;
use itertools::Itertools;
use log::{error, info, warn};
use tokio::{pin, sync::Mutex, time::timeout};

pub struct SeedCrawler {
    dht_client: DhtClientController,
}

impl SeedCrawler {
    pub fn new(dht_client: DhtClientController) -> Self {
        Self { dht_client }
    }

    pub async fn crawl(&mut self) {
        let visited = Arc::new(Mutex::new(HashSet::new()));
        let local_peer_id = self.dht_client.id();
        let sender = self.dht_client.sender();

        while let Some(observation) = self
            .dht_client
            .info_hash_observer()
            .map(|observation| {
                let visited = visited.clone();
                let local_peer_id = local_peer_id.clone();
                let sender = sender.clone();
                async move {
                    let (info_hash, peer_addr) = match observation {
                        crate::dht::InfoHashObservation::WithPeerAddr {
                            info_hash,
                            peer_addr,
                        } => (info_hash, vec![peer_addr]),
                        crate::dht::InfoHashObservation::WithUtpPeerAddr { .. } => {
                            warn!("Got Utp peer, not supported yet");
                            return None;
                        }
                        crate::dht::InfoHashObservation::WithDhtNode { info_hash, node } => {
                            if !visited.lock().await.insert(info_hash.clone()) {
                                return None;
                            }
                            let peers = sender
                                .lock()
                                .await
                                .lookup_peers(&info_hash, Some(&node))
                                .await;
                            if let Err(err) = peers {
                                error!("{}", err);
                                return None;
                            }
                            info!("Found peers for hash {}: {:?}", info_hash, peers);
                            (info_hash, peers.unwrap())
                        }
                    };

                    info!("Start downloading metadata for {:?}", info_hash);
                     fun_name(peer_addr, local_peer_id, info_hash).await
                }
            })
            .buffer_unordered(8)
            .next()
            .await
        {
            match observation {
                Some(metadata) => {
                    let metadata = bendy::value::Value::from_bencode(&metadata);
                    info!("Got seed: {:?}", metadata)
                }
                _ => (),
            }
        }
    }
}

async fn fun_name(peer_addr: Vec<std::net::SocketAddr>, local_peer_id: Arc<DhtNodeId>, info_hash: Arc<InfoHash>) -> Result<Option<Box<[u8]>>,Box<dyn Error>> {
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
                            error!(
                                "Failed to download metadata from {}: {}",
                                addr, err
                            );
                            return Err(None);
                        }
                        result.ok()
                    }
                }
            }
        })
        .buffer_unordered(8)
        .filter_map(|metadata| async { metadata });
    pin!(metadata);
    Ok(metadata.next().await)
}
