use crate::lock_mgr::LockManager;
use crate::operation::{OpType, Operation};
use crate::segment::{Segment, Tuple};
use rand::Rng;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Eq, PartialEq, Default)]
pub struct OperationScheduler;

const OPERATION_COUNT: i32 = 10000_i32;

impl OperationScheduler {
    pub fn op_id() -> String {
        let time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        time.to_string()
    }

    pub fn rand_index(segment_capacity: i32) -> Vec<i32> {
        let mut value_index = vec![];
        let rand_i = rand::thread_rng().gen_range(0_i32..segment_capacity);
        value_index.push(rand_i);
        for idx in 1..3 {
            if rand_i + idx > segment_capacity {
                value_index.push(segment_capacity % (rand_i + idx))
            } else {
                value_index.push(rand_i + idx)
            }
        }
        let rand_j = rand::thread_rng().gen_range(1_i32..segment_capacity);
        value_index.push(rand_j);
        value_index
    }

    pub async fn schedule_with_task(segment: Arc<Segment>, worker_size: i32) {
        let segment_capacity = segment.capacity();
        let mut join_handlers = vec![];
        for worker_num in 0..worker_size {
            let join_handler = tokio::task::spawn(async move {
                println!("curr thread = {:?}", std::thread::current().id());
                for _op_count in 0..OPERATION_COUNT {
                    let ops = OperationScheduler::new_operation(
                        segment_capacity,
                        (
                            format!("{}/{}", OperationScheduler::op_id(), worker_num),
                            format!("{}/{}", OperationScheduler::op_id(), worker_num),
                        ),
                    );
                    let read_op = ops.0;
                    let write_op = ops.1;

                    let read_lock_mgr = LockManager::new(read_op);
                    let write_lock_mgr = LockManager::new(write_op);

                    let read_lock = read_lock_mgr.acquire();
                    let write_lock = write_lock_mgr.acquire();
                    println!("Acquire S_Lock = {:?},X_Lock = {:?}", read_lock, write_lock);
                    let release_rs = read_lock_mgr.release();
                    let write_rs = write_lock_mgr.release();
                    println!("Release S_Lock = {:?},X_Lock = {:?}", release_rs, write_rs);
                }
            });
            join_handlers.push(join_handler);
        }
        for join_wait in join_handlers {
            let _ = join_wait.await;
        }
    }

    pub fn new_operation(
        segment_capacity: i32,
        op_id_pair: (String, String),
    ) -> (Operation, Operation) {
        // [i,i+1,i+2,j]
        let value_index = OperationScheduler::rand_index(segment_capacity);
        let read_tuple = Tuple::empty_tuple(&value_index[0..3]);
        let write_tuple = Tuple::empty_tuple(&[(value_index.len() - 1).try_into().unwrap()]);
        (
            Operation::new(op_id_pair.0, read_tuple.tuple_id, OpType::Read),
            Operation::new(op_id_pair.1, write_tuple.tuple_id, OpType::Write),
        )
    }
}
