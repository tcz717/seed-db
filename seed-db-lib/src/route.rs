pub mod kademila {
    use std::{
        collections::HashSet,
        fmt::Debug,
        sync::Arc,
        time::{Duration, SystemTime},
    };

    use log::info;
    use rand::prelude::IteratorRandom;
    use Option;

    use crate::dht::{DhtNode, DhtNodeId, RouteTable, SharedDhtNode};

    pub struct KademilaRouter<const K: usize> {
        id: Arc<DhtNodeId>,
        root: KBucket<K>,
        node_count: usize,
        bucket_count: usize,
        unheathy: HashSet<DhtNodeId>,
    }

    impl<const K: usize> KademilaRouter<K> {
        pub fn new(id: DhtNodeId) -> Self {
            Self {
                id: id.into(),
                root: KBucket::new(),
                node_count: 0,
                bucket_count: 1,
                unheathy: Default::default(),
            }
        }

        pub fn find_node<'a>(&'a self, id: &DhtNodeId) -> Option<&SharedDhtNode> {
            let mut bit = id.traverse_bits().rev();
            let mut bucket = &self.root;
            loop {
                match bucket {
                    KBucket::Bucket { nodes, .. } => return nodes.iter().find(|n| n.id() == id),
                    KBucket::Branch { low, high } => {
                        bucket = if Some(true) == bit.next() { high } else { low };
                    }
                }
            }
        }

        pub fn drop_node<'a>(&'a mut self, id: &DhtNodeId) {
            let mut bit = id.traverse_bits().rev();
            let mut bucket = &mut self.root;
            loop {
                match bucket {
                    KBucket::Bucket { nodes, .. } => {
                        if let Some(_) = nodes
                            .iter()
                            .position(|n| n.id() == id)
                            .map(|n| nodes.remove(n))
                        {
                            self.node_count -= 1;
                        }
                        return;
                    }
                    KBucket::Branch { low, high } => {
                        bucket = if Some(true) == bit.next() { high } else { low };
                    }
                }
            }
        }

        #[cfg(test)]
        pub(crate) fn inorder_dump(&self) -> Vec<DhtNodeId> {
            let mut ids = vec![];
            Self::dfs(&self.root, &mut ids);
            ids
        }

        #[cfg(test)]
        fn dfs(bucket: &KBucket<K>, ids: &mut Vec<DhtNodeId>) {
            match bucket {
                KBucket::Bucket { nodes, .. } => {
                    ids.extend(nodes.iter().map(|n| n.id.clone()));
                }
                KBucket::Branch { low, high } => {
                    Self::dfs(low.as_ref(), ids);
                    Self::dfs(high.as_ref(), ids);
                }
            };
        }

        fn nearests_core<'a>(
            bucket: &'a KBucket<K>,
            bit: &mut impl Iterator<Item = bool>,
            result: &mut Vec<&'a SharedDhtNode>,
        ) {
            match bucket {
                KBucket::Bucket { nodes, .. } => {
                    let remaining = K - result.len();
                    let take_k = nodes.iter().take(remaining);
                    result.extend(take_k);
                }
                KBucket::Branch { low, high } => {
                    let (first, second) = if bit.next().unwrap() {
                        (high, low)
                    } else {
                        (low, high)
                    };
                    Self::nearests_core(first, bit, result);
                    if result.len() < K {
                        Self::nearests_core(second, bit, result);
                    }
                }
            }
        }

        /// Get a reference to the kademila router's root.
        pub fn root(&self) -> &KBucket<K> {
            &self.root
        }
    }

    impl<const K: usize> Default for KademilaRouter<K> {
        fn default() -> Self {
            Self {
                id: DhtNodeId::random().into(),
                root: KBucket::new(),
                node_count: 0,
                bucket_count: 1,
                unheathy: Default::default(),
            }
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
                        match nodes.iter_mut().find(|n| n.id == node.id) {
                            Some(n) => {
                                *n = Arc::new(node);
                                *last_modified = SystemTime::now();
                                return;
                            }
                            None => {
                                if nodes.len() < K {
                                    info!(
                                        "New node found: {}, {} total",
                                        node,
                                        self.node_count + 1
                                    );
                                    nodes.push(Arc::new(node));
                                    self.node_count += 1;
                                    *last_modified = SystemTime::now();
                                    return;
                                }
                            }
                        }
                        if depth == ROOT_DEPTH || self.id.bit(depth + 1) == node.id.bit(depth + 1) {
                            split_point.set(depth);
                            bucket.split(&split_point);
                            self.bucket_count += 1;
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

        fn id(&self) -> Arc<DhtNodeId> {
            self.id.clone()
        }

        fn nearests(&self, id: &DhtNodeId) -> Vec<&SharedDhtNode> {
            let mut nodes = Vec::with_capacity(K);
            Self::nearests_core(
                &self.root,
                &mut id
                    .as_ref()
                    .iter()
                    .flat_map(|b| (0..8).rev().map(move |bit| *b & (1 << bit) > 0)),
                &mut nodes,
            );
            nodes
        }

        fn unheathy(&self) -> Vec<SharedDhtNode> {
            self.unheathy
                .iter()
                .filter_map(|id| self.find_node(id))
                .cloned()
                .collect()
        }

        fn clean_unheathy(&mut self) -> Vec<SharedDhtNode> {
            let old_nodes = self.unheathy.drain().collect::<Vec<_>>();
            if old_nodes.len() > 0 {
                info!("Cleaned {} nodes", old_nodes.len());
            }
            for node in old_nodes {
                self.drop_node(&node);
            }
            let nodes: Vec<SharedDhtNode> = self
                .root
                .into_iter()
                .pure_buckets()
                .filter_map(|b| match b {
                    KBucket::Bucket {
                        nodes,
                        last_modified,
                    } => {
                        if SystemTime::now() > *last_modified + Duration::from_secs(60 * 15) {
                            nodes.last()
                        } else {
                            None
                        }
                    }
                    _ => None,
                })
                .cloned()
                .collect();
            self.unheathy
                .extend(nodes.iter().map(|node| node.id().clone()));
            nodes
        }

        fn nodes_count(&self) -> usize {
            self.node_count
        }

        fn pick_node(&self) -> Option<&SharedDhtNode> {
            let mut bucket = &self.root;
            loop {
                match bucket {
                    KBucket::Bucket { nodes, .. } => {
                        return nodes.iter().choose(&mut rand::thread_rng())
                    }
                    KBucket::Branch { low, high } => {
                        bucket = if rand::random() { high } else { low };
                    }
                }
            }
        }
    }

    #[derive(Debug)]
    pub enum KBucket<const K: usize> {
        Bucket {
            nodes: Vec<SharedDhtNode>,
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
                        .partition(|node| node.id.as_ref() < split_point.as_ref());
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

        /// Returns `true` if the k_bucket is [`Bucket`].
        pub fn is_bucket(&self) -> bool {
            matches!(self, Self::Bucket { .. })
        }

        /// Returns `true` if the k_bucket is [`Branch`].
        pub fn is_branch(&self) -> bool {
            matches!(self, Self::Branch { .. })
        }
    }

    impl<'a, const K: usize> IntoIterator for &'a KBucket<K> {
        type Item = &'a KBucket<K>;

        type IntoIter = KBucketBucketPreorderIter<'a, K>;

        fn into_iter(self) -> Self::IntoIter {
            KBucketBucketPreorderIter::new(self)
        }
    }

    pub struct KBucketBucketPreorderIter<'a, const K: usize> {
        stack: Vec<&'a KBucket<K>>,
    }

    impl<'a, const K: usize> KBucketBucketPreorderIter<'a, K> {
        pub fn new(root: &'a KBucket<K>) -> Self {
            Self { stack: vec![root] }
        }

        pub fn nodes(self) -> impl Iterator<Item = &'a SharedDhtNode> {
            self.pure_buckets()
                .filter_map(|b| match b {
                    KBucket::Bucket { nodes, .. } => Some(nodes),
                    _ => None,
                })
                .flatten()
        }
        pub fn pure_buckets(self) -> impl Iterator<Item = &'a KBucket<K>> {
            self.filter(|b| b.is_bucket())
        }
    }

    impl<'a, const K: usize> Iterator for KBucketBucketPreorderIter<'a, K> {
        type Item = &'a KBucket<K>;

        fn next(&mut self) -> Option<Self::Item> {
            if let Some(bucket) = self.stack.pop() {
                if let &KBucket::Branch { low, high } = &bucket {
                    self.stack.push(&low);
                    self.stack.push(&high);
                }
                Some(bucket)
            } else {
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

    use itertools::Itertools;

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

    #[test]
    fn kbucket_nearests() {
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

        let target = create_test_node(|bytes| bytes[0] = 0b10100).id().clone();
        let ids = router.inorder_dump();
        let expected: Vec<_> = ids
            .iter()
            .sorted_by_key(|id| *id ^ &target)
            .take(2)
            .collect();
        let result: Vec<_> = router
            .nearests(&create_test_node(|bytes| bytes[0] = 0b10100).id())
            .iter()
            .map(|n| n.id())
            .collect();

        assert_eq!(expected, result);
    }
}
