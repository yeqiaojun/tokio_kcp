use std::sync::{
    atomic::{AtomicUsize, Ordering},
    mpsc, Arc, OnceLock,
};
use std::thread;
use std::time::{Duration, Instant};

use crate::session::KcpSession;

const SESSION_SCHED_RESOLUTION: Duration = Duration::from_millis(10);

struct ScheduledTask {
    session: Arc<KcpSession>,
    deadline_ms: u64,
}

pub(crate) struct FixedTickScheduler {
    shard_txs: Vec<mpsc::Sender<ShardCommand>>,
    next_shard: AtomicUsize,
}

enum ShardCommand {
    Put(ScheduledTask),
}

impl FixedTickScheduler {
    pub(crate) fn new(parallel: usize, resolution: Duration) -> Arc<Self> {
        let parallel = parallel.max(1);
        let mut shard_txs = Vec::with_capacity(parallel);

        for _ in 0..parallel {
            let (tx, rx) = mpsc::channel::<ShardCommand>();
            shard_txs.push(tx);

            thread::spawn(move || {
                let mut tasks = Vec::<ScheduledTask>::new();
                let mut next_tick = Instant::now();

                loop {
                    let now = Instant::now();
                    if now < next_tick {
                        thread::sleep(next_tick - now);
                    }
                    next_tick = Instant::now() + resolution;

                    if drain_commands(&rx, &mut tasks) && tasks.is_empty() {
                        break;
                    }

                    process_tasks(&mut tasks, now_millis_u64());
                }
            });
        }

        Arc::new(FixedTickScheduler {
            shard_txs,
            next_shard: AtomicUsize::new(0),
        })
    }

    pub(crate) fn put(&self, session: Arc<KcpSession>, deadline_ms: u64) {
        let idx = self.next_shard.fetch_add(1, Ordering::Relaxed) % self.shard_txs.len();
        let _ = self.shard_txs[idx].send(ShardCommand::Put(ScheduledTask { session, deadline_ms }));
    }

    pub(crate) fn register(&self, session: Arc<KcpSession>) {
        self.put(session, now_millis_u64());
    }
}

pub(crate) fn session_scheduler() -> &'static Arc<FixedTickScheduler> {
    static SESSION_SCHEDULER: OnceLock<Arc<FixedTickScheduler>> = OnceLock::new();

    SESSION_SCHEDULER.get_or_init(|| FixedTickScheduler::new(default_scheduler_parallel(), SESSION_SCHED_RESOLUTION))
}

fn default_scheduler_parallel() -> usize {
    let n = std::thread::available_parallelism().map_or(1, usize::from);
    n.clamp(2, 4)
}

pub(crate) fn now_millis_u64() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("time went backwards")
        .as_millis() as u64
}

fn apply_command(tasks: &mut Vec<ScheduledTask>, command: ShardCommand) {
    match command {
        ShardCommand::Put(task) => {
            tasks.push(task);
        }
    }
}

fn drain_commands(rx: &mpsc::Receiver<ShardCommand>, tasks: &mut Vec<ScheduledTask>) -> bool {
    loop {
        match rx.try_recv() {
            Ok(command) => apply_command(tasks, command),
            Err(mpsc::TryRecvError::Empty) => return false,
            Err(mpsc::TryRecvError::Disconnected) => return true,
        }
    }
}

fn process_tasks(tasks: &mut Vec<ScheduledTask>, now_ms: u64) {
    let mut idx = 0;
    while idx < tasks.len() {
        if tasks[idx].deadline_ms > now_ms {
            idx += 1;
            continue;
        }

        match tasks[idx].session.poll_scheduler_tick(now_ms) {
            Some(deadline_ms) => {
                tasks[idx].deadline_ms = deadline_ms;
                idx += 1;
            }
            None => {
                tasks.swap_remove(idx);
            }
        }
    }
}
