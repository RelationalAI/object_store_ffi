use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::time::Instant;

use metrics::{histogram, Unit};
use metrics_util::MetricKind::*;
use once_cell::sync::OnceCell;
use serde::{Deserialize, Serialize};

macro_rules! metric_const {
    ($name: ident) => {
        #[allow(non_upper_case_globals)]
        pub(crate) const $name: &'static str = stringify!($name);
    };
}

macro_rules! metric_to_value {
    (Histogram, $map:ident, $name:ident) => {
        $map
            .remove(&CompositeKey::new(Histogram, Key::from_name($name)))
            .map(|(_unit, _shared, value)| {
                let metrics_util::debugging::DebugValue::Histogram(mut vals) = value else { unreachable!("bad metric type") };
                vals.sort();
                let p = |v: f64| {
                    if vals.len() == 0 {
                        0.0f64
                    } else {
                        *vals[((vals.len() as f64 * v).floor() as usize).min(vals.len() - 1)]
                    }
                };
                [p(0.0), p(0.5), p(0.99)]
            })
    };
    (Counter, $map:ident, $name:ident) => {
        $map
            .remove(&CompositeKey::new(Counter, Key::from_name($name)))
            .map(|(_unit, _shared, value)| {
                let metrics_util::debugging::DebugValue::Counter(val) = value else { unreachable!("bad metric type") };
                val
            })
    };
    (Gauge, $map:ident, $name:ident) => {
        $map
            .remove(&CompositeKey::new(Gauge, Key::from_name($name)))
            .map(|(unit, _shared, value)| {
                let metrics_util::debugging::DebugValue::Gauge(val) = value else { unreachable!("bad metric type") };
                val
            })
    };
}

macro_rules! metric_type {
    (Histogram) => {
        [f64; 3]
    };
    (Counter) => {
        u64
    };
    (Gauge) => {
        i64
    };
}


macro_rules! declare_metrics {
    ($($kind:tt, $name:ident, $unit:expr, $desc:literal);+) => {
        $(metric_const!($name);)+

        pub fn init_metrics() {
            $(
                match $kind {
                    Histogram => ::metrics::describe_histogram!($name, $unit, $desc),
                    Counter => ::metrics::describe_counter!($name, $unit, $desc),
                    Gauge => ::metrics::describe_gauge!($name, $unit, $desc)
                }
            )+
        }

        #[derive(Debug, Default, Serialize, Deserialize)]
        #[repr(C)]
        pub struct MetricsSnapshot {
            live_bytes: i64,
            $(
                #[serde(skip_serializing_if = "Option::is_none")]
                $name: Option<metric_type!($kind)>
            ),+
        }

        impl MetricsSnapshot {
            pub fn current() -> MetricsSnapshot {
                use ::metrics::Key;
                use metrics_util::CompositeKey;
                let snapshot = METRICS_SNAPSHOTTER.get().map(|s| s.snapshot());
                if let Some(snapshot) = snapshot {
                    let mut map = snapshot.into_hashmap();
                    MetricsSnapshot {
                        live_bytes: FAST_METRICS.live_bytes.load(Ordering::Relaxed),
                        $(
                            $name: metric_to_value!($kind, map, $name)
                        ),+
                    }
                } else {
                    MetricsSnapshot {
                        live_bytes: FAST_METRICS.live_bytes.load(Ordering::Relaxed),
                        ..Default::default()
                    }
                }
            }
        }
    };
}

declare_metrics! {
    Histogram, get_attempt_duration, Unit::Seconds, "Duration of a get operation attempt";
    Histogram, put_attempt_duration, Unit::Seconds, "Duration of a put operation attempt";
    Histogram, delete_attempt_duration, Unit::Seconds, "Duration of a delete operation attempt";
    Histogram, multipart_get_attempt_duration, Unit::Seconds, "Duration of a multipart get operation attempt";
    Histogram, multipart_put_attempt_duration, Unit::Seconds, "Duration of a multipart put operation attempt";
    Histogram, material_from_metadata_duration, Unit::Seconds, "Time to get a potentially cached key";
    Histogram, material_for_write_duration, Unit::Seconds, "Time to fetch a potentially cached key for writes";
    Histogram, list_attempt_duration, Unit::Seconds, "Duration of a list operation attempt";
    Histogram, sf_token_refresh_duration, Unit::Seconds, "Time to refresh a token from SF";
    Histogram, sf_token_login_duration, Unit::Seconds, "Time to get the first token from SF";
    Histogram, sf_query_attempt_duration, Unit::Seconds, "Time to perform a SF query attempt";
    Histogram, sf_fetch_upload_info_retried_duration, Unit::Seconds, "Time to fetch a new write key from SF";
    Histogram, sf_fetch_path_info_retried_duration, Unit::Seconds, "Time to fetch the key for a path from SF";
    Histogram, sf_get_presigned_url_retried_duration, Unit::Seconds, "Time to fetch a presigned url from SF";
    Histogram, sf_heartbeat_duration, Unit::Seconds, "Duration of the Snowflake heartbeat request";
    Counter, total_retries, Unit::Count, "Total amount of retries";
    Counter, total_sf_retries, Unit::Count, "Total amount of Snowflake client retries";
    Counter, total_get_ops, Unit::Count, "Total amount of object GET operations";
    Counter, total_put_ops, Unit::Count, "Total amount of object PUT operations";
    Counter, total_delete_ops, Unit::Count, "Total amount of object DELETE operations";
    Counter, total_bulk_delete_ops, Unit::Count, "Total amount of object bulk DELETE operations";
    Counter, total_keyring_get, Unit::Count, "Total amount of key fetches from in-memory keyring";
    Counter, total_keyring_miss, Unit::Count, "Total amount of misses while fetching key from keyring";
    Counter, total_fetch_upload_info, Unit::Count, "Total amount of Snowflake stage info requests";
    Counter, total_fetch_path_info, Unit::Count, "Total amount of Snowflake GET requests to fetch keys"
}

static METRICS_SNAPSHOTTER: OnceCell<metrics_util::debugging::Snapshotter> = OnceCell::new();

pub fn setup_recorder() {
    let recorder = metrics_util::debugging::DebuggingRecorder::new();
    METRICS_SNAPSHOTTER.set(recorder.snapshotter())
        .map_err(|_| ())
        .expect("failed to set metrics snapshotter");
    recorder.install().expect("failed to install metrics recorder");
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
pub struct FastMetrics {
    live_bytes: AtomicI64
}

impl FastMetrics {
    pub const fn new() -> Self {
        FastMetrics {
            live_bytes: AtomicI64::new(0)
        }
    }
}

pub static FAST_METRICS: FastMetrics = FastMetrics::new();

thread_local! {
    static LOCAL_METRICS: std::cell::Cell<i64> = const { std::cell::Cell::new(0) };
}

#[cold]
fn flush(v: i64) {
    FAST_METRICS.live_bytes.fetch_add(v, Ordering::AcqRel);
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
