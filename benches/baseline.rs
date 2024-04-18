use std::{time::{Duration, Instant}, sync::atomic::AtomicBool};

use criterion::{criterion_group, criterion_main, Criterion};

pub static STOP: AtomicBool = AtomicBool::new(false);

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("system allocation", |b| {
        // This simply measures the overhead of using the system allocator normally.
        b.iter(|| Vec::<String>::with_capacity(128));
    });

    c.bench_function("system allocation multithreaded", |b| {
        // This simply measures the overhead of using the system allocator normally.
        b.iter_custom(|iters| {
            let start = Instant::now();
            let n = 4;
            let mut handles = vec![];
            for _t in 0..n {
                handles.push(std::thread::spawn(move || {
                    for _i in 0..(iters / n) {
                        criterion::black_box(Vec::<String>::with_capacity(128));
                    }
                }));
            }

            for handle in handles {
                handle.join().unwrap();
            }

            start.elapsed()
        });
    });
    c.bench_function("system allocation incremental", |b| {
        // This simply measures the overhead of using the system allocator normally.
        b.iter_custom(|iters| {
            let mut data = Vec::with_capacity(100 * 1024 * 1024);
            let start = Instant::now();
            for i in 0..iters {
                if data.len() < 100 * 1024 * 1024 {
                    data.push(Box::new(i));
                } else {
                    data.clear();
                    data.push(Box::new(i));
                }
            }
            start.elapsed()
        });
    });
}

criterion_group!(
    name = benches;
    config = Criterion::default()
        .significance_level(0.02)
        .noise_threshold(0.05)
        .measurement_time(Duration::from_secs(10))
        .warm_up_time(Duration::from_secs(3));
    targets = criterion_benchmark
);
criterion_main!(benches);
