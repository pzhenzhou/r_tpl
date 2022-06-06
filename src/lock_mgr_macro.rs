#[macro_export]
macro_rules! declare_locks_table {
    ($struct_name:ident; $field_name:ty) => {
        #[derive(Debug, Clone)]
        /// Table_key Could be Operation ResourceId op_id.
        pub(crate) struct $struct_name {
            table_key: $field_name,
            locks: std::sync::Arc<parking_lot::RwLock<Vec<Lock>>>,
        }

        impl $struct_name {
            pub fn new(input_arg: $field_name) -> Self {
                Self {
                    table_key: input_arg,
                    locks: std::sync::Arc::new(parking_lot::RwLock::new(vec![])),
                }
            }

            /// check resource held lock conflict
            pub fn lock_conflicts(&self, require_lock: LockMode) -> bool {
                let locks_vec = &*self.locks.read();
                for lock in locks_vec.iter() {
                    if !lock.lock_mode.compatible(require_lock) {
                        return true;
                    }
                }
                false
            }

            pub fn lock_size(&self) -> usize {
                let lock_guard = &*self.locks.read();
                lock_guard.len()
            }

            pub fn remove_lock(&self, input_rid: ResourceId) {
                let mut lock_vec = self.locks.write();
                let vec_lock = &mut *lock_vec;
                vec_lock.retain(|lock| lock.rid != input_rid);
            }

            pub fn update_lock(&self, new_lock: Lock, input_rid: ResourceId) {
                let locks_vec = &mut *self.locks.write();
                let mut replace_id = 0_i32;
                for (idx, ele) in locks_vec.iter().enumerate() {
                    if ele.rid == input_rid {
                        replace_id = idx as i32;
                        break;
                    }
                }
                if replace_id >= 0 {
                    locks_vec.remove(replace_id as usize);
                    locks_vec.insert(replace_id as usize, new_lock);
                }
            }

            pub fn add_lock(&self, new_lock: Lock) {
                let locks_vec = &mut *self.locks.write();
                locks_vec.push(new_lock);
            }

            pub fn get_lock_mode(&self, input_rid: ResourceId) -> LockMode {
                let locks_vec = &*self.locks.read();
                for lock in locks_vec.iter() {
                    if lock.rid == input_rid {
                        return lock.lock_mode.clone();
                    }
                }
                LockMode::NoLock
            }
        }
    };
}
