pub mod kademila {
    use crate::dht::{DhtNode, DhtNodeId};

    pub struct KademilaRouter<const K: usize> {
        id: DhtNodeId,
        root: KBucket<K>,
    }

    pub enum KBucket<const K: usize> {
        Bucket(Box<[DhtNode; K]>),
        Branch {
            low: Box<KBucket<K>>,
            high: Box<KBucket<K>>,
        },
    }
}
