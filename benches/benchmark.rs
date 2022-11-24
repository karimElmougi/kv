use criterion::{black_box, criterion_group, criterion_main, Criterion};
use tempfile::NamedTempFile;

fn worst_case(c: &mut Criterion) {
    let f = NamedTempFile::new().unwrap();
    let store = kv::Store::<String>::open(f.path()).unwrap();

    let data = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Vestibulum rhoncus ligula a consectetur cursus.".to_string();

    for _ in 0..100_000 {
        store.set("key1", &data).unwrap();
    }
    c.bench_function("search worst case", |b| {
        b.iter(|| store.get(black_box("key1")))
    });
}

criterion_group!(benches, worst_case);
criterion_main!(benches);
