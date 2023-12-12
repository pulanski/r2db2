use common::PAGE_SIZE;
use criterion::{criterion_group, criterion_main, Criterion};
use std::fs;
use storage::disk::DiskManager;

fn write_benchmark(c: &mut Criterion) {
    let dm = DiskManager::new("testdata/bench.db").unwrap();
    let data = vec![0u8; PAGE_SIZE];

    c.bench_function("write_page", |b| {
        b.iter(|| {
            let page_id = rand::random::<u32>();
            dm.write_page(page_id, &data).unwrap();
        })
    });

    fs::remove_file("testdata/bench.db").unwrap();
}

criterion_group!(benches, write_benchmark);
criterion_main!(benches);
