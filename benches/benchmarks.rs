use criterion::{black_box, criterion_group, criterion_main, Criterion};
use ddb_core::lexer::Tokenizer;

fn tokenize_benchmark(c: &mut Criterion) {
    c.bench_function("tokenize_select", |b| {
        b.iter(|| {
            let mut tokenizer = Tokenizer::new();
            tokenizer.tokenize(black_box("SELECT * FROM users WHERE id = 123"))
        })
    });
}

criterion_group!(benches, tokenize_benchmark);
criterion_main!(benches);
