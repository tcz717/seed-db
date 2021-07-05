pub mod kademila {
    use std::{collections::LinkedList, time::SystemTime};

    use crate::dht::{DhtNode, DhtNodeId, RouteTable};

    pub struct KademilaRouter<const K: usize> {
        id: DhtNodeId,
        root: KBucket<K>,
    }

    impl<const K: usize> KademilaRouter<K> {
        pub fn new(id: DhtNodeId) -> Self {
            Self {
                id,
                root: KBucket::new(),
            }
        }
    }

    impl<const K: usize> RouteTable for KademilaRouter<K> {
        fn update(&mut self, node: DhtNode) {
            let mut bucket = &mut self.root;
            let mut left = DhtNodeId::zered();
            let mut right = DhtNodeId::max();
            let mut split_point = (&left >> 1) + (&right >> 1);

            loop {
                match bucket {
                    KBucket::Bucket {
                        nodes,
                        last_modified,
                    } => {
                        if nodes.len() < K {
                            match nodes.iter_mut().find(|n| n.id == node.id) {
                                Some(n) => *n = node,
                                None => nodes.push_front(node),
                            }
                            *last_modified = SystemTime::now();
                            return;
                        }
                        if self.id >= left && self.id <= right {
                            bucket.split(&split_point)
                        } else {
                            return;
                        }
                    }
                    KBucket::Branch { low, high } => {
                        todo!()
                    }
                }
            }
        }

        fn get_nearests<I>(&self, id: &DhtNodeId) -> I
        where
            I: Iterator<Item = DhtNode>,
        {
            todo!()
        }
    }

    pub enum KBucket<const K: usize> {
        Bucket {
            nodes: LinkedList<DhtNode>,
            last_modified: SystemTime,
        },
        Branch {
            low: Box<KBucket<K>>,
            high: Box<KBucket<K>>,
        },
    }

    impl<const K: usize> KBucket<K> {
        pub fn new() -> Self {
            Self::Bucket {
                nodes: Default::default(),
                last_modified: SystemTime::now(),
            }
        }

        pub fn split(&mut self, split_point: &DhtNodeId) {
            match self {
                KBucket::Bucket {
                    nodes,
                    last_modified,
                } => {
                    let last_modified = *last_modified;
                    let (low, high) = nodes
                        .split_off(0)
                        .into_iter()
                        .partition(|node| node.id.as_ref() < split_point);
                    *self = KBucket::Branch {
                        low: Box::new(KBucket::Bucket {
                            nodes: low,
                            last_modified,
                        }),
                        high: Box::new(KBucket::Bucket {
                            nodes: high,
                            last_modified,
                        }),
                    };
                }
                _ => (),
            }
        }

        pub fn update(&mut self, node: DhtNode) {
            todo!()
        }
    }
}
