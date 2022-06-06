use parking_lot::RwLock;
use petgraph::graph::DiGraph;
use std::sync::Arc;

#[derive(Debug, Copy, Clone, Default, Eq, PartialEq, Hash)]
pub struct LockNode<'a> {
    op: &'a str,
    rid: &'a str,
}

/// If there are mutual references between nodes (incoming outgoing),
/// or if there is a circle between nodes, there will be a deadlock
pub struct DealLockDetector {
    lock_graph: Arc<RwLock<DiGraph<String, String>>>,
}

impl DealLockDetector {
    fn link_node() {
        todo!()
    }

    fn find_parent() -> String {
        todo!()
    }

    fn has_incoming() -> bool {
        todo!()
    }

    fn has_outgoing() -> bool {
        todo!()
    }
}
