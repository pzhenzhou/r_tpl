#![allow(clippy::map_entry)]

use crate::declare_locks_table;
use crate::lock::{Lock, LockMode, OP_LOCK_MAPPING};
use crate::lock_mgr::LockErrorCode::*;
use crate::operation::Operation;
use crate::segment::ResourceId;
use anyhow::{anyhow, Result};
use once_cell::sync::Lazy;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::thread::park_timeout;
use std::time::{Duration, Instant};
use thiserror::Error;

declare_locks_table!(OperationLockTable; Operation);
declare_locks_table!(ResourceLockTable; ResourceId);

static GLOBAL_LOCK_TABLE: Lazy<RwLock<LockTable>> = Lazy::new(|| {
    let lock_table = LockTable::new();
    RwLock::new(lock_table)
});

#[derive(Debug, Default, Clone)]
pub struct LockTable {
    resource_table: HashMap<String, ResourceLockTable>,
    operation_table: HashMap<String, OperationLockTable>,
}

impl LockTable {
    pub fn new() -> Self {
        Self {
            resource_table: HashMap::new(),
            operation_table: HashMap::new(),
        }
    }
}

#[derive(Error, Debug)]
pub enum LockErrorCode {
    #[error("Failed acquire for OP_ID {0}. Lock already exist.")]
    DuplicateLock(String),
    #[error("Failed No transaction holds a lock  for OP_ID {0}")]
    NoLockHeld(String),
    #[error("Acquire Lock conflicts OP_ID {0} RES_ID {1}")]
    LockConflicts(String, String),
}

#[derive(Debug, Clone)]
pub struct LockManager {
    operation: Operation,
}

impl Default for LockManager {
    fn default() -> Self {
        Self::new(Operation::default())
    }
}

impl LockManager {
    pub fn new(operation: Operation) -> Self {
        Self { operation }
    }

    pub fn try_acquire(&self, retry_time_count: Duration) -> Result<Lock> {
        if retry_time_count.as_millis() == 0 {
            self.acquire()
        } else {
            let start_park = Instant::now();
            let mut timeout_remaining = retry_time_count;
            loop {
                park_timeout(timeout_remaining);
                let elapsed = start_park.elapsed();
                if elapsed >= retry_time_count {
                    break;
                }
                if let Ok(lock) = self.acquire() {
                    return Ok(lock);
                } else {
                    timeout_remaining = retry_time_count - elapsed;
                }
            }
            self.acquire()
        }
    }

    pub fn acquire(&self) -> Result<Lock> {
        let rid = self.operation.clone().resources;
        let op_id = self.operation.id.clone();
        let require_lock = *OP_LOCK_MAPPING.get(&self.operation.op_type).unwrap();

        let lock_table = &mut *GLOBAL_LOCK_TABLE.write();
        let op_locks_table = &mut lock_table.operation_table;
        let resource_lock_table = &mut lock_table.resource_table;
        if op_locks_table.contains_key(&op_id) {
            Err(anyhow!(DuplicateLock(op_id)))
        } else {
            let lock = if resource_lock_table.contains_key(&rid) {
                let res_table = resource_lock_table.get(&rid).unwrap();
                let old_lock = res_table.get_lock_mode(rid.clone());
                let new_lock = Lock::new(require_lock, op_id.clone(), rid.clone());
                // RW
                if old_lock.upgradable(require_lock) {
                    self.promote(new_lock.clone(), resource_lock_table, op_locks_table);
                } else {
                    if !old_lock.compatible(require_lock) {
                        // TODO: if lock conflicts may be wait for other op to release lock.
                        return Err(anyhow!(LockConflicts(op_id, rid)));
                    }
                    res_table.add_lock(new_lock.clone());
                    let ops_table = OperationLockTable::new(self.operation.clone());
                    ops_table.add_lock(new_lock.clone());
                    op_locks_table.insert(op_id, ops_table);
                }
                new_lock
            } else {
                let new_lock = Lock::new(require_lock, op_id.clone(), rid.clone());
                // add new_lock for locks table
                let res_table = ResourceLockTable::new(rid.clone());
                res_table.add_lock(new_lock.clone());
                let ops_table = OperationLockTable::new(self.operation.clone());
                ops_table.add_lock(new_lock.clone());
                resource_lock_table.insert(rid, res_table);
                op_locks_table.insert(op_id, ops_table);
                new_lock
            };
            Ok(lock)
        }
    }

    pub fn release(&self) -> Result<()> {
        let op_id = self.operation.clone().id;
        let lock_table = &mut *GLOBAL_LOCK_TABLE.write();
        let op_locks_table = &mut lock_table.operation_table;
        let resource_lock_table = &mut lock_table.resource_table;
        // println!("Release = {:?} {:?}", op_locks_table.clone(), resource_lock_table.clone());
        if !op_locks_table.contains_key(&op_id) {
            Err(anyhow!(NoLockHeld(op_id)))
        } else {
            let rid = self.operation.clone().resources;
            let res_table = resource_lock_table.get_mut(&rid).unwrap();
            res_table.remove_lock(rid.clone());
            if res_table.lock_size() == 0_usize {
                resource_lock_table.remove(&rid);
            }
            op_locks_table.remove(&op_id);
            Ok(())
        }
    }

    fn promote(
        &self,
        new_lock: Lock,
        resource_lock_table: &mut HashMap<String, ResourceLockTable>,
        op_locks_table: &mut HashMap<String, OperationLockTable>,
    ) {
        println!(
            "LockManager lock promote. {:?} {:?}",
            self.operation, new_lock
        );
        let rid = self.operation.clone().resources;
        let res_table = resource_lock_table.get(&rid).unwrap();
        let ops_table = op_locks_table.get(&self.operation.id).unwrap();

        res_table.update_lock(new_lock.clone(), rid.clone());
        ops_table.update_lock(new_lock, rid.clone());
    }
}

#[cfg(test)]
mod tests {
    use crate::lock_mgr::{LockManager, GLOBAL_LOCK_TABLE};
    use crate::operation::OpType::*;
    use crate::operation::Operation;
    use std::time::Duration;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    pub async fn test_acquire_multi_state() {
        let mut join_handlers = vec![];
        for idx in 0..2_i32 {
            let join = tokio::task::spawn(async move {
                let lock_mgr =
                    LockManager::new(Operation::new(idx.to_string(), "1,2,3".to_string(), Read));
                let lock = lock_mgr.acquire();
                assert!(lock.is_ok());
                println!("lock_test_rs = {:?}", lock);
            });
            join_handlers.push(join);
        }
        let handlers_await = futures::future::join_all(join_handlers);
        let _await_rs = handlers_await.await;
        let final_lock_table = &*GLOBAL_LOCK_TABLE.read();
        let rs_table = &final_lock_table.resource_table;
        let op_table = &final_lock_table.operation_table;
        assert_eq!(1, rs_table.len());
        assert_eq!(2, op_table.len());
        println!("LockTable = {:#?}", final_lock_table);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    pub async fn test_wait_lock_state() {
        let resource_id = "1,2,3".to_string();
        let read_op_id = "1".to_string();
        let write_op_id = "2".to_string();
        let write_op = Operation::new(write_op_id, resource_id.clone(), Write);
        let read_op = Operation::new(read_op_id, resource_id.clone(), Read);

        let (tx, mut rx) = tokio::sync::mpsc::channel(1);

        tokio::task::spawn(async move {
            let recv_write_lock = rx.recv().await;
            println!(
                "receive write lock success acquire READ_LOCK lock = {:?}",
                recv_write_lock
            );
            let lock_mgr = LockManager::new(read_op);
            //  loop {
            //      if let Ok(lock) = lock_mgr.acquire() {
            //          println!("S Lock success.lock = {:?}", lock);
            //          break;
            //      } else {
            //          println!("S Lock Err");
            //      }
            //      std::thread::sleep(Duration::from_millis(10));
            //  }
            let lock_rs = lock_mgr.try_acquire(Duration::from_millis(50));
            println!("S Lock lock = {:?}", lock_rs);
            assert!(lock_rs.is_ok());
        });

        let write_lock_join = tokio::task::spawn(async move {
            let write_lock_mgr = LockManager::new(write_op);
            let write_lock_rs = write_lock_mgr.acquire();
            assert!(write_lock_rs.is_ok());
            let send_lock_rs = tx.send(write_lock_rs.unwrap()).await;
            send_lock_rs.unwrap();
            println!("X Lock Acquire Success");
            write_lock_mgr
        });
        let write_mgr = write_lock_join.await;
        if let Ok(lock_mgr) = write_mgr {
            std::thread::sleep(Duration::from_millis(50));
            let write_release = lock_mgr.release();
            assert!(write_release.is_ok());
            println!("X Lock Release Success");
        }
    }

    #[test]
    pub fn test_lock_unlock() {
        let lock_mgr = LockManager::new(Operation::new("1".to_string(), "1,2,3".to_string(), Read));
        let lock_rs = lock_mgr.acquire();
        assert!(lock_rs.is_ok());
        let unlock_rs = lock_mgr.release();
        assert!(unlock_rs.is_ok());
    }
}
