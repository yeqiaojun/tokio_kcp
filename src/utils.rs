use std::time::{SystemTime, UNIX_EPOCH};

#[inline]
pub fn now_millis_u64() -> u64 {
    let start = SystemTime::now();
    let since_the_epoch = start.duration_since(UNIX_EPOCH).expect("time went afterwards");
    since_the_epoch.as_millis() as u64
}
