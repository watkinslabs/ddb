use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};
use ddb_core::config::{Column, DataType, Table};
use ddb_core::engine::QueryExecutor;
use ddb_core::parser::Parser as SqlParser;
use ddb_core::lexer::Tokenizer;
use ddb_core::parser::Statement;
use std::io::{Write as IoWrite};
use std::sync::Arc;
use std::thread;
use tempfile::NamedTempFile;

// ============================================================================
// Concurrent File Locking Benchmarks
// ============================================================================

fn create_test_table_with_rows(num_rows: usize) -> (Table, NamedTempFile) {
    let mut file = NamedTempFile::new().unwrap();
    writeln!(file, "id,name,value").unwrap();

    for i in 1..=num_rows {
        writeln!(file, "{},User{},{}", i, i, i * 100).unwrap();
    }
    file.flush().unwrap();

    let table = Table {
        name: "test".to_string(),
        database: "test".to_string(),
        data_file: file.path().to_string_lossy().to_string(),
        columns: vec![
            Column {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
            },
            Column {
                name: "name".to_string(),
                data_type: DataType::String,
                nullable: false,
            },
            Column {
                name: "value".to_string(),
                data_type: DataType::Integer,
                nullable: false,
            },
        ],
        field_delimiter: ',',
        data_starts_on: 0,
        comment_char: None,
    };

    (table, file)
}

fn parse_query(query: &str) -> Statement {
    let mut tokenizer = Tokenizer::new();
    let tokens = tokenizer.tokenize(query).unwrap();
    let mut parser = SqlParser::new(tokens);
    parser.parse().unwrap()
}

/// Benchmark concurrent reads (file locking with shared locks)
fn bench_concurrent_reads(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_reads");

    for num_threads in [2, 4, 8].iter() {
        group.bench_with_input(
            BenchmarkId::new("threads", num_threads),
            num_threads,
            |b, &num_threads| {
                b.iter_batched(
                    || {
                        let (table, _file) = create_test_table_with_rows(1000);
                        let table_arc = Arc::new(table);
                        let query = Arc::new(parse_query("SELECT * FROM test WHERE value > 50000"));
                        (table_arc, _file, query)
                    },
                    |(table, _file, query)| {
                        let mut handles = vec![];

                        for _ in 0..num_threads {
                            let table_clone = table.clone();
                            let query_clone = query.clone();

                            let handle = thread::spawn(move || {
                                let executor = QueryExecutor::new();
                                if let Statement::Select(select_stmt) = (*query_clone).clone() {
                                    executor.execute_select(&select_stmt, &table_clone).unwrap()
                                } else {
                                    panic!("Expected SELECT statement");
                                }
                            });

                            handles.push(handle);
                        }

                        // Wait for all threads to complete
                        for handle in handles {
                            handle.join().unwrap();
                        }
                    },
                    BatchSize::SmallInput,
                )
            },
        );
    }

    group.finish();
}

/// Benchmark sequential writes (file locking with exclusive locks)
fn bench_sequential_writes(c: &mut Criterion) {
    let mut group = c.benchmark_group("sequential_writes");

    for num_ops in [5, 10, 20].iter() {
        group.bench_with_input(
            BenchmarkId::new("operations", num_ops),
            num_ops,
            |b, &num_ops| {
                b.iter_batched(
                    || {
                        let (table, _file) = create_test_table_with_rows(100);
                        (table, _file)
                    },
                    |(table, _file)| {
                        let executor = QueryExecutor::new();

                        // Perform sequential updates (each gets exclusive lock)
                        for i in 0..num_ops {
                            let query = format!("UPDATE test SET value = {} WHERE id = {}", (i + 1) * 1000, (i % 100) + 1);
                            let stmt = parse_query(&query);

                            if let Statement::Update(update_stmt) = stmt {
                                executor.execute_update(&update_stmt, &table).unwrap();
                            }
                        }
                    },
                    BatchSize::SmallInput,
                )
            },
        );
    }

    group.finish();
}

/// Benchmark INSERT operations with file locking
fn bench_concurrent_inserts(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_inserts");

    for num_ops in [5, 10].iter() {
        group.bench_with_input(
            BenchmarkId::new("operations", num_ops),
            num_ops,
            |b, &num_ops| {
                b.iter_batched(
                    || {
                        let (table, _file) = create_test_table_with_rows(100);
                        (table, _file)
                    },
                    |(table, _file)| {
                        let executor = QueryExecutor::new();

                        // Sequential inserts (each gets exclusive lock)
                        for i in 0..num_ops {
                            let query = format!(
                                "INSERT INTO test (id, name, value) VALUES ({}, 'NewUser{}', {})",
                                1000 + i,
                                i,
                                i * 100
                            );
                            let stmt = parse_query(&query);

                            if let Statement::Insert(insert_stmt) = stmt {
                                executor.execute_insert(&insert_stmt, &table).unwrap();
                            }
                        }
                    },
                    BatchSize::SmallInput,
                )
            },
        );
    }

    group.finish();
}

/// Benchmark UPSERT operations (mix of reads and writes with locking)
fn bench_upsert_locking(c: &mut Criterion) {
    let mut group = c.benchmark_group("upsert_locking");

    for size in [100, 500, 1000].iter() {
        group.bench_with_input(
            BenchmarkId::new("table_size", size),
            size,
            |b, &size| {
                b.iter_batched(
                    || {
                        let (table, _file) = create_test_table_with_rows(size);
                        (table, _file)
                    },
                    |(table, _file)| {
                        let executor = QueryExecutor::new();

                        // UPSERT requires reading entire file, finding key, then writing
                        let stmt = parse_query(
                            "UPSERT INTO test (id, name, value) VALUES (50, 'UpsertUser', 9999) ON id"
                        );

                        if let Statement::Upsert(upsert_stmt) = stmt {
                            executor.execute_upsert(&upsert_stmt, &table).unwrap();
                        }
                    },
                    BatchSize::SmallInput,
                )
            },
        );
    }

    group.finish();
}

/// Benchmark DELETE operations with file locking (full file rewrite)
fn bench_delete_locking(c: &mut Criterion) {
    let mut group = c.benchmark_group("delete_locking");

    for size in [100, 500, 1000].iter() {
        group.bench_with_input(
            BenchmarkId::new("table_size", size),
            size,
            |b, &size| {
                b.iter_batched(
                    || {
                        let (table, _file) = create_test_table_with_rows(size);
                        (table, _file)
                    },
                    |(table, _file)| {
                        let executor = QueryExecutor::new();

                        // DELETE requires reading entire file and rewriting
                        let stmt = parse_query("DELETE FROM test WHERE value < 5000");

                        if let Statement::Delete(delete_stmt) = stmt {
                            executor.execute_delete(&delete_stmt, &table).unwrap();
                        }
                    },
                    BatchSize::SmallInput,
                )
            },
        );
    }

    group.finish();
}

/// Benchmark mixed read/write workload
fn bench_mixed_workload(c: &mut Criterion) {
    let mut group = c.benchmark_group("mixed_workload");

    for size in [100, 500].iter() {
        group.bench_with_input(
            BenchmarkId::new("table_size", size),
            size,
            |b, &size| {
                b.iter_batched(
                    || {
                        let (table, _file) = create_test_table_with_rows(size);
                        (table, _file)
                    },
                    |(table, _file)| {
                        let executor = QueryExecutor::new();

                        // Mix of operations that require different lock types
                        // 1. SELECT (shared lock)
                        let select_stmt = parse_query("SELECT * FROM test WHERE value > 50000");
                        if let Statement::Select(s) = select_stmt {
                            executor.execute_select(&s, &table).unwrap();
                        }

                        // 2. UPDATE (exclusive lock)
                        let update_stmt = parse_query("UPDATE test SET value = 99999 WHERE id = 1");
                        if let Statement::Update(u) = update_stmt {
                            executor.execute_update(&u, &table).unwrap();
                        }

                        // 3. SELECT again (shared lock)
                        let select_stmt2 = parse_query("SELECT COUNT(*) FROM test");
                        if let Statement::Select(s) = select_stmt2 {
                            executor.execute_select(&s, &table).unwrap();
                        }

                        // 4. INSERT (exclusive lock)
                        let insert_stmt = parse_query("INSERT INTO test (id, name, value) VALUES (9999, 'New', 500)");
                        if let Statement::Insert(i) = insert_stmt {
                            executor.execute_insert(&i, &table).unwrap();
                        }
                    },
                    BatchSize::SmallInput,
                )
            },
        );
    }

    group.finish();
}

criterion_group!(
    concurrency_benches,
    bench_concurrent_reads,
    bench_sequential_writes,
    bench_concurrent_inserts,
    bench_upsert_locking,
    bench_delete_locking,
    bench_mixed_workload,
);

criterion_main!(concurrency_benches);
