use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicI64, Ordering};

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
            let live_bytes = cell.get() + (new_size - layout.size()) as i64;
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
