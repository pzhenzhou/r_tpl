use r_tpl::operation_scheduler::OperationScheduler;
use r_tpl::segment::Segment;
use std::sync::Arc;

const DATA_SIZE: i32 = 100000;
const WORKER_NUM: i32 = 4;

#[tokio::main(flavor = "multi_thread", worker_threads = 6)]
async fn main() {
    let ints: [i32; DATA_SIZE as usize] = (1..=DATA_SIZE)
        .collect::<Vec<_>>()
        .try_into()
        .expect("size error iter");
    let segment = Segment::from_ints(10000, &ints, "DefaultSegmentId".to_string());
    OperationScheduler::schedule_with_task(Arc::new(segment), WORKER_NUM).await;
}
