use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::time::Instant;

use metrics::{describe_histogram, histogram, Unit};

macro_rules! metric_const {
    ($name: ident) => {
        #[allow(non_upper_case_globals)]
        pub(crate) const $name: &'static str = stringify!($name);
    };
}

metric_const!(get_attempt_duration);
metric_const!(put_attempt_duration);
metric_const!(delete_attempt_duration);
metric_const!(multipart_get_attempt_duration);
metric_const!(multipart_put_attempt_duration);
metric_const!(material_for_write_duration);
metric_const!(material_from_metadata_duration);
metric_const!(list_attempt_duration);
metric_const!(sf_heartbeat_duration);
metric_const!(sf_token_refresh_duration);
metric_const!(sf_token_login_duration);
metric_const!(sf_query_attempt_duration);
metric_const!(sf_fetch_upload_info_retried_duration);
metric_const!(sf_fetch_path_info_retried_duration);
metric_const!(sf_get_presigned_url_retried_duration);
metric_const!(total_get_ops);
metric_const!(total_put_ops);
metric_const!(total_delete_ops);
metric_const!(total_keyring_get);
metric_const!(total_keyring_miss);
metric_const!(total_fetch_upload_info);
metric_const!(total_fetch_path_info);

#[allow(dead_code)]
pub fn init_metrics() {
    describe_histogram!(get_attempt_duration, Unit::Seconds, "Duration of a get operation attempt");
    describe_histogram!(put_attempt_duration, Unit::Seconds, "Duration of a put operation attempt");
    describe_histogram!(delete_attempt_duration, Unit::Seconds, "Duration of a delete operation attempt");
    describe_histogram!(multipart_get_attempt_duration, Unit::Seconds, "Duration of a multipart get operation attempt");
    describe_histogram!(multipart_put_attempt_duration, Unit::Seconds, "Duration of a multipart put operation attempt");
    describe_histogram!(material_from_metadata_duration, Unit::Seconds, "Time to get a potentially cached key");
    describe_histogram!(material_for_write_duration, Unit::Seconds, "Time to fetch a potentially cached key for writes");
    describe_histogram!(list_attempt_duration, Unit::Seconds, "Duration of a list operation attempt");
    describe_histogram!(sf_token_refresh_duration, Unit::Seconds, "Time to refresh a token from SF");
    describe_histogram!(sf_token_login_duration, Unit::Seconds, "Time to get the first token from SF");
    describe_histogram!(sf_query_attempt_duration, Unit::Seconds, "Time to perform a SF query attempt");
    describe_histogram!(sf_fetch_upload_info_retried_duration, Unit::Seconds, "Time to fetch a new write key from SF");
    describe_histogram!(sf_fetch_path_info_retried_duration, Unit::Seconds, "Time to fetch the key for a path from SF");
    describe_histogram!(sf_get_presigned_url_retried_duration, Unit::Seconds, "Time to fetch a presigned url from SF");
}

pub(crate) struct DurationGuard {
    name: &'static str,
    t0: Instant,
    discarded: AtomicBool
}
impl DurationGuard {
    pub(crate) fn new(name: &'static str) -> DurationGuard {
        DurationGuard { name, t0: Instant::now(), discarded: AtomicBool::new(false) }
    }
    pub(crate) fn discard(&self) {
        self.discarded.store(true, Ordering::Relaxed)
    }
}

impl Drop for DurationGuard {
    fn drop(&mut self) {
        if !self.discarded.load(Ordering::Relaxed) {
            histogram!(self.name).record(Instant::now() - self.t0);
        }
    }
}

#[macro_export]
macro_rules! duration_on_drop {
    ($name: expr) => {
        crate::metrics::DurationGuard::new($name)
    };
}

#[derive(Debug, Default)]
#[repr(C)]
pub struct Metrics {
    live_bytes: AtomicI64
}

#[repr(C)]
pub struct MetricsSnapshot {
    live_bytes: i64
}

impl MetricsSnapshot {
    pub fn current() -> MetricsSnapshot {
        MetricsSnapshot {
            live_bytes: METRICS.live_bytes.load(Ordering::Relaxed)
        }
    }
}

impl Metrics {
    pub const fn new() -> Self {
        Metrics {
            live_bytes: AtomicI64::new(0)
        }
    }
}

pub static METRICS: Metrics = Metrics::new();

thread_local! {
    static LOCAL_METRICS: std::cell::Cell<i64> = const { std::cell::Cell::new(0) };
}

#[cold]
fn flush(v: i64) {
    METRICS.live_bytes.fetch_add(v, Ordering::AcqRel);
}

pub struct InstrumentedAllocator;
unsafe impl GlobalAlloc for InstrumentedAllocator {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        LOCAL_METRICS.with(|cell| {
            let live_bytes = cell.get() + (l.size() as i64);
            if live_bytes  < 100 * 1024 {
                cell.set(live_bytes);
            } else {
                flush(live_bytes);
                cell.set(0);
            }
        });
        System.alloc(l)
    }

    unsafe fn dealloc(&self, ptr: *mut u8, l: Layout) {
        LOCAL_METRICS.with(|cell| {
            let live_bytes = cell.get() - (l.size() as i64);
            if live_bytes  > - (100 * 1024) {
                cell.set(live_bytes);
            } else {
                flush(live_bytes);
                cell.set(0);
            }
        });
        System.dealloc(ptr, l);
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        LOCAL_METRICS.with(|cell| {
            let live_bytes = cell.get() + (new_size as i64) - (layout.size() as i64);
            if live_bytes.abs() < 100 * 1024 {
                cell.set(live_bytes);
            } else {
                flush(live_bytes);
                cell.set(0);
            }
        });
        System.realloc(ptr, layout, new_size)
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        LOCAL_METRICS.with(|cell| {
            let live_bytes = cell.get() + (layout.size() as i64);
            if live_bytes  < 100 * 1024 {
                cell.set(live_bytes);
            } else {
                flush(live_bytes);
                cell.set(0);
            }
        });
        System.alloc_zeroed(layout)
    }
}
