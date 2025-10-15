use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion, BenchmarkId};
use ddb_core::config::{Column, DataType, Table, TableCatalog};
use ddb_core::engine::QueryExecutor;
use ddb_core::lexer::Tokenizer;
use ddb_core::parser::Parser as SqlParser;
use ddb_core::parser::Statement;
use std::io::{Write as IoWrite};
use tempfile::NamedTempFile;

// ============================================================================
// Helper functions to create test data
// ============================================================================

fn create_test_table_with_rows(num_rows: usize) -> (Table, NamedTempFile) {
    let mut file = NamedTempFile::new().unwrap();
    writeln!(file, "id,name,age,salary,department").unwrap();

    for i in 1..=num_rows {
        writeln!(
            file,
            "{},User{},{},{},dept{}",
            i,
            i,
            20 + (i % 50),
            30000 + (i % 100000),
            (i % 10) + 1
        ).unwrap();
    }
    file.flush().unwrap();

    let table = Table {
        name: "users".to_string(),
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
                name: "age".to_string(),
                data_type: DataType::Integer,
                nullable: false,
            },
            Column {
                name: "salary".to_string(),
                data_type: DataType::Integer,
                nullable: false,
            },
            Column {
                name: "department".to_string(),
                data_type: DataType::String,
                nullable: false,
            },
        ],
        field_delimiter: ',',
        data_starts_on: 0,
        comment_char: None,
    };

    (table, file)
}

fn create_orders_table(num_rows: usize) -> (Table, NamedTempFile) {
    let mut file = NamedTempFile::new().unwrap();
    writeln!(file, "order_id,user_id,amount,status").unwrap();

    for i in 1..=num_rows {
        writeln!(
            file,
            "{},{},{},{}",
            i,
            (i % 1000) + 1, // user_id cycles through 1-1000
            (i % 10000) + 10,
            if i % 2 == 0 { "completed" } else { "pending" }
        ).unwrap();
    }
    file.flush().unwrap();

    let table = Table {
        name: "orders".to_string(),
        database: "test".to_string(),
        data_file: file.path().to_string_lossy().to_string(),
        columns: vec![
            Column {
                name: "order_id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
            },
            Column {
                name: "user_id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
            },
            Column {
                name: "amount".to_string(),
                data_type: DataType::Integer,
                nullable: false,
            },
            Column {
                name: "status".to_string(),
                data_type: DataType::String,
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

// ============================================================================
// Tokenization Benchmarks
// ============================================================================

fn bench_tokenization(c: &mut Criterion) {
    let mut group = c.benchmark_group("tokenization");

    group.bench_function("simple_select", |b| {
        b.iter(|| {
            let mut tokenizer = Tokenizer::new();
            tokenizer.tokenize(black_box("SELECT * FROM users WHERE id = 123"))
        })
    });

    group.bench_function("complex_select", |b| {
        b.iter(|| {
            let mut tokenizer = Tokenizer::new();
            tokenizer.tokenize(black_box(
                "SELECT u.name, COUNT(o.id) as order_count, SUM(o.amount) as total \
                 FROM users u INNER JOIN orders o ON u.id = o.user_id \
                 WHERE u.age > 25 AND o.status = 'completed' \
                 GROUP BY u.name HAVING COUNT(o.id) > 5 \
                 ORDER BY total DESC LIMIT 10"
            ))
        })
    });

    group.bench_function("insert", |b| {
        b.iter(|| {
            let mut tokenizer = Tokenizer::new();
            tokenizer.tokenize(black_box(
                "INSERT INTO users (id, name, age, salary) VALUES (1, 'John', 30, 50000)"
            ))
        })
    });

    group.bench_function("update", |b| {
        b.iter(|| {
            let mut tokenizer = Tokenizer::new();
            tokenizer.tokenize(black_box(
                "UPDATE users SET salary = 55000, age = 31 WHERE id = 123"
            ))
        })
    });

    group.bench_function("delete", |b| {
        b.iter(|| {
            let mut tokenizer = Tokenizer::new();
            tokenizer.tokenize(black_box("DELETE FROM users WHERE age < 18"))
        })
    });

    group.finish();
}

// ============================================================================
// SELECT Benchmarks (varying data sizes)
// ============================================================================

fn bench_select_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("select");

    for size in [100, 1000, 10000].iter() {
        group.bench_with_input(BenchmarkId::new("full_scan", size), size, |b, &size| {
            b.iter_batched(
                || {
                    let (table, _file) = create_test_table_with_rows(size);
                    (table, parse_query("SELECT * FROM users"))
                },
                |(table, stmt)| {
                    let executor = QueryExecutor::new();
                    if let Statement::Select(select_stmt) = stmt {
                        executor.execute_select(&select_stmt, &table).unwrap()
                    } else {
                        panic!("Expected SELECT statement");
                    }
                },
                BatchSize::SmallInput,
            )
        });

        group.bench_with_input(BenchmarkId::new("where_filter", size), size, |b, &size| {
            b.iter_batched(
                || {
                    let (table, _file) = create_test_table_with_rows(size);
                    (table, parse_query("SELECT * FROM users WHERE age > 30 AND salary > 50000"))
                },
                |(table, stmt)| {
                    let executor = QueryExecutor::new();
                    if let Statement::Select(select_stmt) = stmt {
                        executor.execute_select(&select_stmt, &table).unwrap()
                    } else {
                        panic!("Expected SELECT statement");
                    }
                },
                BatchSize::SmallInput,
            )
        });

        group.bench_with_input(BenchmarkId::new("order_by", size), size, |b, &size| {
            b.iter_batched(
                || {
                    let (table, _file) = create_test_table_with_rows(size);
                    (table, parse_query("SELECT * FROM users ORDER BY salary DESC"))
                },
                |(table, stmt)| {
                    let executor = QueryExecutor::new();
                    if let Statement::Select(select_stmt) = stmt {
                        executor.execute_select(&select_stmt, &table).unwrap()
                    } else {
                        panic!("Expected SELECT statement");
                    }
                },
                BatchSize::SmallInput,
            )
        });

        group.bench_with_input(BenchmarkId::new("order_by_limit", size), size, |b, &size| {
            b.iter_batched(
                || {
                    let (table, _file) = create_test_table_with_rows(size);
                    (table, parse_query("SELECT * FROM users ORDER BY salary DESC LIMIT 10"))
                },
                |(table, stmt)| {
                    let executor = QueryExecutor::new();
                    if let Statement::Select(select_stmt) = stmt {
                        executor.execute_select(&select_stmt, &table).unwrap()
                    } else {
                        panic!("Expected SELECT statement");
                    }
                },
                BatchSize::SmallInput,
            )
        });
    }

    group.finish();
}

// ============================================================================
// Aggregation Benchmarks
// ============================================================================

fn bench_aggregations(c: &mut Criterion) {
    let mut group = c.benchmark_group("aggregation");

    for size in [100, 1000, 10000].iter() {
        group.bench_with_input(BenchmarkId::new("count", size), size, |b, &size| {
            b.iter_batched(
                || {
                    let (table, _file) = create_test_table_with_rows(size);
                    (table, parse_query("SELECT COUNT(*) FROM users"))
                },
                |(table, stmt)| {
                    let executor = QueryExecutor::new();
                    if let Statement::Select(select_stmt) = stmt {
                        executor.execute_select(&select_stmt, &table).unwrap()
                    } else {
                        panic!("Expected SELECT statement");
                    }
                },
                BatchSize::SmallInput,
            )
        });

        group.bench_with_input(BenchmarkId::new("sum_avg", size), size, |b, &size| {
            b.iter_batched(
                || {
                    let (table, _file) = create_test_table_with_rows(size);
                    (table, parse_query("SELECT SUM(salary), AVG(age) FROM users"))
                },
                |(table, stmt)| {
                    let executor = QueryExecutor::new();
                    if let Statement::Select(select_stmt) = stmt {
                        executor.execute_select(&select_stmt, &table).unwrap()
                    } else {
                        panic!("Expected SELECT statement");
                    }
                },
                BatchSize::SmallInput,
            )
        });

        group.bench_with_input(BenchmarkId::new("group_by", size), size, |b, &size| {
            b.iter_batched(
                || {
                    let (table, _file) = create_test_table_with_rows(size);
                    (table, parse_query("SELECT department, COUNT(*), AVG(salary) FROM users GROUP BY department"))
                },
                |(table, stmt)| {
                    let executor = QueryExecutor::new();
                    if let Statement::Select(select_stmt) = stmt {
                        executor.execute_select(&select_stmt, &table).unwrap()
                    } else {
                        panic!("Expected SELECT statement");
                    }
                },
                BatchSize::SmallInput,
            )
        });

        group.bench_with_input(BenchmarkId::new("group_by_having", size), size, |b, &size| {
            b.iter_batched(
                || {
                    let (table, _file) = create_test_table_with_rows(size);
                    (table, parse_query(
                        "SELECT department, COUNT(*) as cnt, AVG(salary) as avg_sal \
                         FROM users GROUP BY department HAVING COUNT(*) > 5"
                    ))
                },
                |(table, stmt)| {
                    let executor = QueryExecutor::new();
                    if let Statement::Select(select_stmt) = stmt {
                        executor.execute_select(&select_stmt, &table).unwrap()
                    } else {
                        panic!("Expected SELECT statement");
                    }
                },
                BatchSize::SmallInput,
            )
        });
    }

    group.finish();
}

// ============================================================================
// JOIN Benchmarks
// ============================================================================

fn bench_joins(c: &mut Criterion) {
    let mut group = c.benchmark_group("join");

    for size in [100, 500, 1000].iter() {
        group.bench_with_input(BenchmarkId::new("inner_join", size), size, |b, &size| {
            b.iter_batched(
                || {
                    let (users_table, _users_file) = create_test_table_with_rows(size);
                    let (orders_table, _orders_file) = create_orders_table(size * 2);

                    let mut catalog = TableCatalog::new();
                    let _ = catalog.add_table(users_table.clone());
                    let _ = catalog.add_table(orders_table);

                    let query = parse_query(
                        "SELECT u.name, o.amount FROM users u \
                         INNER JOIN orders o ON u.id = o.user_id"
                    );

                    (users_table, catalog, query)
                },
                |(table, catalog, stmt)| {
                    let executor = QueryExecutor::new();
                    if let Statement::Select(select_stmt) = stmt {
                        executor.execute_select_with_catalog(&select_stmt, &table, Some(&catalog)).unwrap()
                    } else {
                        panic!("Expected SELECT statement");
                    }
                },
                BatchSize::SmallInput,
            )
        });

        group.bench_with_input(BenchmarkId::new("left_join", size), size, |b, &size| {
            b.iter_batched(
                || {
                    let (users_table, _users_file) = create_test_table_with_rows(size);
                    let (orders_table, _orders_file) = create_orders_table(size / 2);

                    let mut catalog = TableCatalog::new();
                    let _ = catalog.add_table(users_table.clone());
                    let _ = catalog.add_table(orders_table);

                    let query = parse_query(
                        "SELECT u.name, o.amount FROM users u \
                         LEFT JOIN orders o ON u.id = o.user_id"
                    );

                    (users_table, catalog, query)
                },
                |(table, catalog, stmt)| {
                    let executor = QueryExecutor::new();
                    if let Statement::Select(select_stmt) = stmt {
                        executor.execute_select_with_catalog(&select_stmt, &table, Some(&catalog)).unwrap()
                    } else {
                        panic!("Expected SELECT statement");
                    }
                },
                BatchSize::SmallInput,
            )
        });
    }

    group.finish();
}

// ============================================================================
// INSERT Benchmarks
// ============================================================================

fn bench_insert_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert");

    for batch_size in [1, 10, 100].iter() {
        group.bench_with_input(BenchmarkId::new("batch", batch_size), batch_size, |b, &batch_size| {
            b.iter_batched(
                || {
                    let (table, _file) = create_test_table_with_rows(0);

                    let mut values_str = String::new();
                    for i in 0..batch_size {
                        if i > 0 {
                            values_str.push_str(", ");
                        }
                        values_str.push_str(&format!("({}, 'User{}', 30, 50000, 'dept1')", i + 1, i + 1));
                    }

                    let query = format!("INSERT INTO users (id, name, age, salary, department) VALUES {}", values_str);
                    (table, parse_query(&query))
                },
                |(table, stmt)| {
                    let executor = QueryExecutor::new();
                    if let Statement::Insert(insert_stmt) = stmt {
                        executor.execute_insert(&insert_stmt, &table).unwrap()
                    } else {
                        panic!("Expected INSERT statement");
                    }
                },
                BatchSize::SmallInput,
            )
        });
    }

    group.finish();
}

// ============================================================================
// UPDATE Benchmarks
// ============================================================================

fn bench_update_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("update");

    for size in [100, 1000, 10000].iter() {
        group.bench_with_input(BenchmarkId::new("single_row", size), size, |b, &size| {
            b.iter_batched(
                || {
                    let (table, _file) = create_test_table_with_rows(size);
                    (table, parse_query("UPDATE users SET salary = 60000 WHERE id = 50"))
                },
                |(table, stmt)| {
                    let executor = QueryExecutor::new();
                    if let Statement::Update(update_stmt) = stmt {
                        executor.execute_update(&update_stmt, &table).unwrap()
                    } else {
                        panic!("Expected UPDATE statement");
                    }
                },
                BatchSize::SmallInput,
            )
        });

        group.bench_with_input(BenchmarkId::new("multiple_rows", size), size, |b, &size| {
            b.iter_batched(
                || {
                    let (table, _file) = create_test_table_with_rows(size);
                    (table, parse_query("UPDATE users SET salary = salary * 1.1 WHERE age > 30"))
                },
                |(table, stmt)| {
                    let executor = QueryExecutor::new();
                    if let Statement::Update(update_stmt) = stmt {
                        executor.execute_update(&update_stmt, &table).unwrap()
                    } else {
                        panic!("Expected UPDATE statement");
                    }
                },
                BatchSize::SmallInput,
            )
        });
    }

    group.finish();
}

// ============================================================================
// DELETE Benchmarks
// ============================================================================

fn bench_delete_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("delete");

    for size in [100, 1000, 10000].iter() {
        group.bench_with_input(BenchmarkId::new("single_row", size), size, |b, &size| {
            b.iter_batched(
                || {
                    let (table, _file) = create_test_table_with_rows(size);
                    (table, parse_query("DELETE FROM users WHERE id = 50"))
                },
                |(table, stmt)| {
                    let executor = QueryExecutor::new();
                    if let Statement::Delete(delete_stmt) = stmt {
                        executor.execute_delete(&delete_stmt, &table).unwrap()
                    } else {
                        panic!("Expected DELETE statement");
                    }
                },
                BatchSize::SmallInput,
            )
        });

        group.bench_with_input(BenchmarkId::new("multiple_rows", size), size, |b, &size| {
            b.iter_batched(
                || {
                    let (table, _file) = create_test_table_with_rows(size);
                    (table, parse_query("DELETE FROM users WHERE age < 30"))
                },
                |(table, stmt)| {
                    let executor = QueryExecutor::new();
                    if let Statement::Delete(delete_stmt) = stmt {
                        executor.execute_delete(&delete_stmt, &table).unwrap()
                    } else {
                        panic!("Expected DELETE statement");
                    }
                },
                BatchSize::SmallInput,
            )
        });
    }

    group.finish();
}

// ============================================================================
// UPSERT Benchmarks
// ============================================================================

fn bench_upsert_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("upsert");

    for size in [100, 1000].iter() {
        group.bench_with_input(BenchmarkId::new("insert_new", size), size, |b, &size| {
            b.iter_batched(
                || {
                    let (table, _file) = create_test_table_with_rows(size);
                    (table, parse_query(&format!(
                        "UPSERT INTO users (id, name, age, salary, department) \
                         VALUES ({}, 'NewUser', 25, 45000, 'dept5') ON id",
                        size + 1000
                    )))
                },
                |(table, stmt)| {
                    let executor = QueryExecutor::new();
                    if let Statement::Upsert(upsert_stmt) = stmt {
                        executor.execute_upsert(&upsert_stmt, &table).unwrap()
                    } else {
                        panic!("Expected UPSERT statement");
                    }
                },
                BatchSize::SmallInput,
            )
        });

        group.bench_with_input(BenchmarkId::new("update_existing", size), size, |b, &size| {
            b.iter_batched(
                || {
                    let (table, _file) = create_test_table_with_rows(size);
                    (table, parse_query(
                        "UPSERT INTO users (id, name, age, salary, department) \
                         VALUES (50, 'UpdatedUser', 35, 70000, 'dept1') ON id"
                    ))
                },
                |(table, stmt)| {
                    let executor = QueryExecutor::new();
                    if let Statement::Upsert(upsert_stmt) = stmt {
                        executor.execute_upsert(&upsert_stmt, &table).unwrap()
                    } else {
                        panic!("Expected UPSERT statement");
                    }
                },
                BatchSize::SmallInput,
            )
        });
    }

    group.finish();
}

// ============================================================================
// Register all benchmarks
// ============================================================================

criterion_group!(
    benches,
    bench_tokenization,
    bench_select_operations,
    bench_aggregations,
    bench_joins,
    bench_insert_operations,
    bench_update_operations,
    bench_delete_operations,
    bench_upsert_operations,
);

criterion_main!(benches);
