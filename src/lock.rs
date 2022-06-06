use crate::operation::OpType;
use crate::segment::ResourceId;
use lazy_static::lazy_static;
use std::collections::HashMap;

lazy_static! {
    pub static ref OP_LOCK_MAPPING: HashMap<OpType, LockMode> = {
        let mut op_lock_mapping = HashMap::new();
        op_lock_mapping.insert(OpType::Read, LockMode::Shared);
        op_lock_mapping.insert(OpType::Write, LockMode::Exclusive);
        op_lock_mapping.insert(OpType::NoOp, LockMode::NoLock);
        op_lock_mapping
    };
}

#[macro_export]
macro_rules! get_lock_mode {
    ($owners:expr, $rid:expr) => {{
        let lock_mode = if $owners.contains_key($rid) {
            let operation = $owners.get($rid).unwrap();
            OP_LOCK_MAPPING.get(&operation.op_type()).unwrap()
        } else {
            OP_LOCK_MAPPING.get(&OpType::NoOp).unwrap()
        };
        lock_mode
    }};
}

#[derive(Debug, Copy, Clone, Hash, Eq, PartialEq)]
pub enum LockMode {
    Shared,
    Exclusive,
    NoLock,
}

impl LockMode {
    pub fn compatible(&self, require_lock: LockMode) -> bool {
        use LockMode::*;
        match *self {
            Shared => require_lock == Shared,
            Exclusive => false,
            NoLock => true,
        }
    }

    pub fn upgradable(&self, require_lock: LockMode) -> bool {
        use LockMode::*;
        match *self {
            Shared => require_lock == Exclusive,
            _ => false,
        }
    }
}

impl Default for LockMode {
    fn default() -> Self {
        LockMode::NoLock
    }
}

#[derive(Debug, Clone, Default, Eq, PartialEq, Hash)]
pub struct Lock {
    pub op_id: String,
    pub lock_mode: LockMode,
    pub rid: ResourceId,
}

impl Lock {
    pub fn new(lock_mode: LockMode, op_id: String, rid: ResourceId) -> Self {
        Self {
            op_id,
            lock_mode,
            rid,
        }
    }
}
