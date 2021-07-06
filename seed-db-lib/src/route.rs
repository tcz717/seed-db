pub mod kademila {
    use std::{collections::LinkedList, fmt::Debug, time::SystemTime};

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

        pub(crate) fn inorder_dump(&self) -> Vec<DhtNodeId> {
            let mut ids = vec![];
            Self::dfs(&self.root, &mut ids);
            ids
        }

        fn dfs(bucket: &KBucket<K>, ids: &mut Vec<DhtNodeId>) {
            match bucket {
                KBucket::Bucket { nodes, .. } => {
                    ids.extend(nodes.iter().map(|n| n.id.as_ref().clone()));
                }
                KBucket::Branch { low, high } => {
                    Self::dfs(low.as_ref(), ids);
                    Self::dfs(high.as_ref(), ids);
                }
            };
        }

        /// Get a reference to the kademila router's root.
        pub fn root(&self) -> &KBucket<K> {
            &self.root
        }
    }

    impl<const K: usize> RouteTable for KademilaRouter<K> {
        fn update(&mut self, node: DhtNode) {
            const ROOT_DEPTH: usize = (DhtNodeId::BITS - 1) as usize;
            let mut bucket = &mut self.root;
            let mut depth = ROOT_DEPTH;
            let mut split_point = DhtNodeId::zered();

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
                        if depth == ROOT_DEPTH || self.id.bit(depth + 1) == node.id.bit(depth + 1) {
                            split_point.set(depth);
                            bucket.split(&split_point);
                        } else {
                            return;
                        }
                    }
                    KBucket::Branch { low, high } => {
                        if node.id.bit(depth) {
                            bucket = high;
                        } else {
                            bucket = low;
                        }
                        split_point.write(depth, self.id.bit(depth));
                        depth -= 1;
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

        fn id(&self) -> &DhtNodeId {
            &self.id
        }
    }

    #[derive(Debug)]
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
    }
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

    use crate::dht::{DhtNode, DhtNodeId, RouteTable};

    use super::kademila::KademilaRouter;

    fn create_test_node(f: impl Fn(&mut [u8; 20])) -> DhtNode {
        let id = DhtNodeId::new(&{
            let mut bytes = [0u8; 20];
            f(&mut bytes);
            bytes
        });
        DhtNode::new(
            id,
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 1234)),
        )
    }

    #[test]
    fn kbucket_update() {
        let node = DhtNodeId::new(&{
            let mut bytes = [0u8; 20];
            bytes[0] = 0b10101;
            bytes
        });
        let mut router = KademilaRouter::<2>::new(node);

        router.update(create_test_node(|bytes| bytes[0] = 0b10101));
        router.update(create_test_node(|bytes| bytes[0] = 0b00100));
        router.update(create_test_node(|bytes| bytes[0] = 0b10100));
        router.update(create_test_node(|bytes| bytes[0] = 0b11100));
        router.update(create_test_node(|bytes| bytes[0] = 0b10001));
        router.update(create_test_node(|bytes| bytes[0] = 0b10000));
        router.update(create_test_node(|bytes| bytes[0] = 0b10010));

        let ids = router.inorder_dump();

        println!("{:#?}", router.root());

        assert!(
            ids.iter()
                .fold((None, true), |(last, inc), id| {
                    if let Some(last) = last {
                        return (Some(id), inc && last <= id);
                    } else {
                        return (Some(id), inc);
                    }
                })
                .1
        )
    }
}
