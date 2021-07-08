use seed_db_lib::{dht::DhtClient, route::kademila::KademilaRouter};
use tokio::runtime::Builder;

fn main() {
    let rt = Builder::new_multi_thread().enable_io().build().unwrap();

    rt.block_on(async {
        let mut client: DhtClient<KademilaRouter<8>> = DhtClient::new().unwrap();
        client
            .add_trackers(&[
                "router.bittorrent.com:6881",
                "router.utorrent.com:6881",
                "router.bitcomet.com:6881",
                "dht.transmissionbt.com:6881",
                "dht.aelitis.com:6881",
            ])
            .await;
        client.run().await
    })
    .unwrap();
}
