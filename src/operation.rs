use crate::segment::ResourceId;

#[derive(Eq, PartialEq, Hash, Debug, Clone)]
pub enum OpType {
    Read,
    Write,
    NoOp,
}

impl Default for OpType {
    fn default() -> Self {
        OpType::NoOp
    }
}

#[derive(Debug, Hash, Eq, PartialEq, Clone)]
pub struct Operation {
    pub op_type: OpType,
    pub id: String,
    pub resources: ResourceId,
}

impl Default for Operation {
    fn default() -> Self {
        Self {
            op_type: OpType::NoOp,
            id: "_NONE".to_string(),
            resources: "_NONE_RID".to_string(),
        }
    }
}

impl Operation {
    pub fn new(id: String, rid: ResourceId, op_type: OpType) -> Self {
        Self {
            op_type,
            id,
            resources: rid,
        }
    }
}
