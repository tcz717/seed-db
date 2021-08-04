use log::LevelFilter;
use seed_db_lib::{crawler::SeedCrawler, dht::DhtClient, route::kademila::KademilaRouter};
use tokio::{runtime::Builder, signal::ctrl_c};

fn main() {
    pretty_env_logger::formatted_builder()
        .filter_level(LevelFilter::Info)
        .init();

    let rt = Builder::new_multi_thread().enable_all().build().unwrap();

    rt.block_on(async {
        let mut client: DhtClient<KademilaRouter<8>> = DhtClient::new().await.unwrap();
        client
            .add_trackers(&[
                "router.bittorrent.com:6881",
                "router.utorrent.com:6881",
                "router.bitcomet.com:6881",
                "dht.transmissionbt.com:6881",
                "dht.aelitis.com:6881",
            ])
            .await;
        let controller = client.run();
        let mut crawler = SeedCrawler::new(controller);

        crawler.crawl().await;
        ctrl_c().await
    })
    .unwrap();
}
