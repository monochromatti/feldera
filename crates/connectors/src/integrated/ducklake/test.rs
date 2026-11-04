//! The DuckLake connectors as a pipeline sees them.
//!
//! The unit tests and the catalog-level tests in `tests_live` cover the protocol
//! itself — the decision, the guard row, the change window.  What they cannot
//! cover is composition: that the writer publishes the step the controller hands
//! it, that the reader's resume point survives the controller's stepping, and
//! that a pipeline with a DuckLake source and a DuckLake sink moves rows from one
//! catalog to the other.  That needs a real engine, which is what this suite is.
//!
//! The catalogs are file-backed temp directories, so no service is declared: a
//! suite that needed PostgreSQL would have to skip itself wherever PostgreSQL is
//! not running, and file-backed is the configuration the connector's own docs
//! call out as the single-process one.

use std::io::Write;

use feldera_adapterlib::test::{Updates, wait};
use feldera_types::{program_schema::Relation, transport::ducklake::DuckLakeWriterConfig};
use serde_json::json;
use tempfile::TempDir;

use super::catalog::Catalog;
use crate::test::{
    Service, engine_test,
    pipeline::{PipelineTestEngine, TestCircuit},
    published_schema, read,
};

/// The relation the cases run over: the one the static fixture compiles into a
/// record type (`controller_rows`, with the columns of `relations::basic`).  A
/// relation the fixture does not serve cannot be used at all, so the name is
/// not ours to choose — only the columns of the catalog table are.
fn relation() -> Relation {
    crate::test::suites::controller::relation()
}

/// A catalog in a fresh temp directory, with the extension vendored rather than
/// downloaded.  The suite skips nothing if the extension is missing: DuckDB
/// would go to the network, which a test must never do.
fn catalog_config(dir: &TempDir) -> serde_json::Value {
    let mut config = json!({
        "catalog": format!("ducklake:{}/meta.ducklake", dir.path().display()),
        "data_path": format!("{}/data", dir.path().display()),
    });
    if let Ok(extension_directory) = std::env::var("DUCKLAKE_EXTENSION_DIR") {
        config["extension_directory"] = json!(extension_directory);
    }
    config
}

/// The writer configuration for one output group over one table.
fn writer_config(dir: &TempDir, table: &str) -> serde_json::Value {
    let mut config = catalog_config(dir);
    config["output_group"] = json!("suite");
    config["generation"] = json!("gen-1");
    config["tables"] = json!({ "test_output1": { "table": table } });
    config
}

/// The `orders` table as the engine's `basic` relation spells it.
fn create_table(catalog: &mut Catalog, table: &str) {
    catalog
        .execute_sql(&format!(
            "CREATE TABLE dl.main.{table} (id BIGINT, b BOOLEAN, i BIGINT, s VARCHAR)"
        ))
        .expect("create the table the writer inserts into");
}

/// Prove the writer works by opening the catalog exactly as the connector does.
fn writer(dir: &TempDir, table: &str) -> DuckLakeWriterConfig {
    serde_json::from_value(writer_config(dir, table)).expect("a writer config")
}

/// A step's rows reach the catalog, and the guard row records the step — the
/// output half of the protocol, through the controller rather than around it.
#[engine_test]
pub fn ducklake_output_publishes_the_step_it_committed(engine: &dyn PipelineTestEngine) {
    let dir = TempDir::new().expect("a temp directory");

    // The writer inserts into an existing table; provisioning the catalog is the
    // deployment's job, not the connector's.
    let mut catalog = Catalog::open(&writer(&dir, "orders")).expect("attach the catalog");
    create_table(&mut catalog, "orders");
    drop(catalog);

    let input = tempfile::NamedTempFile::new().expect("a temp file");
    input
        .as_file()
        .write_all(
            json!([
                {"insert": {"id": 1, "b": true, "i": 10, "s": "a"}},
                {"insert": {"id": 2, "b": false, "i": null, "s": "b"}},
            ])
            .to_string()
            .as_bytes(),
        )
        .expect("write the input");

    let config = serde_json::from_value(json!({
        "name": "ducklake_output",
        "workers": 1,
        "inputs": {
            "rows": {
                "paused": true,
                "stream": "test_input1",
                "transport": { "name": "file_input", "config": { "path": input.path() } },
                "format": {
                    "name": "json",
                    "config": { "update_format": "insert_delete", "array": true }
                }
            }
        },
        "outputs": {
            "test_output1": {
                "stream": "test_output1",
                "transport": { "name": "ducklake_output", "config": writer_config(&dir, "orders") }
            }
        }
    }))
    .expect("a pipeline config");

    let (error_sender, error_receiver) = crossbeam::channel::unbounded();
    let controller = engine
        .controller(
            &TestCircuit::identity(relation()),
            &config,
            Box::new(move |error, _| {
                let _ = error_sender.send(format!("ducklake_output suite: {error}"));
            }),
        )
        .expect("start the controller");
    controller.start();
    controller
        .start_input_endpoint("rows")
        .expect("start the input endpoint");

    wait(
        || controller.status().num_total_processed_records() == 2 || !error_receiver.is_empty(),
        30_000,
    )
    .expect("the step to be processed");
    assert!(
        error_receiver.is_empty(),
        "the connector failed: {:?}",
        error_receiver.try_iter().collect::<Vec<_>>()
    );
    controller.stop().expect("stop the controller");

    let mut catalog = Catalog::open(&writer(&dir, "orders")).expect("reattach the catalog");
    assert_eq!(
        catalog
            .scalar("SELECT count(*) FROM dl.main.orders")
            .expect("count the rows"),
        Some("2".to_owned()),
        "both rows of the step are in the catalog"
    );
    // Which step number the rows landed in is the engine's business — it depends
    // on how many steps the controller took before the batch arrived, so the
    // suite must not predict it.  What is this suite's business is that a step
    // was recorded at all, together with the rows themselves: the exact
    // bookkeeping is covered by the protocol tests, which control the step.
    let frontier = catalog.frontier();
    assert!(
        frontier >= 1,
        "the guard row records the step that published: {frontier}"
    );
}

/// A pipeline that reads a DuckLake table: the whole table once, then only what
/// changed — through the controller, with its own resume point.
#[engine_test]
pub fn ducklake_input_bootstraps_and_then_follows(engine: &dyn PipelineTestEngine) {
    let dir = TempDir::new().expect("a temp directory");

    let mut catalog = Catalog::open(&writer(&dir, "orders")).expect("attach the catalog");
    create_table(&mut catalog, "orders");
    catalog
        .execute_sql("INSERT INTO dl.main.orders VALUES (1, true, 10, 'a'), (2, false, NULL, 'b')")
        .expect("seed the table");
    drop(catalog);

    let mut reader = catalog_config(&dir);
    reader["table"] = json!("orders");
    reader["poll_interval"] = json!("50ms");

    let output = tempfile::NamedTempFile::new().expect("a temp file");
    let config = serde_json::from_value(json!({
        "name": "ducklake_input",
        "workers": 1,
        "inputs": {
            "rows": {
                "stream": "test_input1",
                "transport": { "name": "ducklake_input", "config": reader },
            }
        },
        "outputs": {
            "test_output1": {
                "stream": "test_output1",
                "transport": { "name": "file_output", "config": { "path": output.path() } },
                "format": {
                    "name": "json",
                    "config": { "update_format": "insert_delete" }
                }
            }
        }
    }))
    .expect("a pipeline config");

    let (error_sender, error_receiver) = crossbeam::channel::unbounded();
    let controller = engine
        .controller(
            &TestCircuit::identity(relation()),
            &config,
            Box::new(move |error, _| {
                let _ = error_sender.send(format!("ducklake_input suite: {error}"));
            }),
        )
        .expect("start the controller");
    controller.start();
    // A source connector is only driven once its endpoint is started: the
    // controller sends the `Extend` that makes the first read happen.
    controller
        .start_input_endpoint("rows")
        .expect("start the input endpoint");

    wait(
        || controller.status().num_total_processed_records() == 2 || !error_receiver.is_empty(),
        30_000,
    )
    .expect("the bootstrap to be processed");
    assert!(
        error_receiver.is_empty(),
        "the connector failed: {:?}",
        error_receiver.try_iter().collect::<Vec<_>>()
    );

    // A commit made while the pipeline runs arrives as the change feed, not as a
    // second snapshot of the table.
    let mut catalog = Catalog::open(&writer(&dir, "orders")).expect("reattach the catalog");
    catalog
        .execute_sql("INSERT INTO dl.main.orders VALUES (3, true, 30, 'c')")
        .expect("commit a new snapshot");
    drop(catalog);

    wait(
        || controller.status().num_total_processed_records() >= 3 || !error_receiver.is_empty(),
        30_000,
    )
    .expect("the new commit to be followed");
    assert!(
        error_receiver.is_empty(),
        "the connector failed: {:?}",
        error_receiver.try_iter().collect::<Vec<_>>()
    );
    controller.stop().expect("stop the controller");

    let expected = Updates::from_json(
        published_schema(&relation()),
        &[
            (1, r#"{"id": 1, "b": true, "i": 10, "s": "a"}"#),
            (1, r#"{"id": 2, "b": false, "i": null, "s": "b"}"#),
            (1, r#"{"id": 3, "b": true, "i": 30, "s": "c"}"#),
        ],
    )
    .expect("expected rows");
    let actual = read::insert_delete_updates(&relation(), &std::fs::read(output.path()).unwrap())
        .expect("parse the output file");
    assert_eq!(
        actual, expected,
        "the bootstrap, then only the row the new commit added"
    );
}

/// A catalog in a fresh database on the `Postgres` service, as an `ATTACH`
/// string.
///
/// DuckLake does not create the database it stores its metadata in (measured:
/// `Unable to connect to Postgres … database "dl_probe" does not exist`), so the
/// case creates one through a plain `postgres` connection, exactly as a
/// deployment would.
fn postgres_catalog(prefix: &str) -> Option<String> {
    let admin = Service::Postgres.address();
    let url = url::Url::parse(&admin).expect("the Postgres service address is a URL");
    let name = format!(
        "{prefix}_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("the clock is after the epoch")
            .as_nanos()
    );
    // No server is a skip, not a failure: this is the only case in the suite that
    // needs one, and a suite that fails where PostgreSQL is absent would be
    // failing for the environment rather than for the connector.
    let Ok(mut admin) = postgres::Client::connect(&admin, postgres::NoTls) else {
        return None;
    };
    admin
        .batch_execute(&format!("CREATE DATABASE {name} TEMPLATE template0"))
        .expect("create the catalog's database");
    Some(format!(
        "ducklake:postgres:dbname={name} host={} port={} user={} password={}",
        url.host_str().unwrap_or("127.0.0.1"),
        url.port().unwrap_or(5432),
        url.username(),
        url.password().unwrap_or_default(),
    ))
}

/// The writer configuration for one output group over a PostgreSQL catalog.
fn postgres_writer_config(dsn: &str, data: &TempDir, table: &str) -> serde_json::Value {
    let mut config = json!({
        "catalog": dsn,
        "data_path": format!("{}/data", data.path().display()),
        "output_group": "suite",
        "generation": "gen-1",
        "tables": { "test_output1": { "table": table } },
    });
    if let Ok(extension_directory) = std::env::var("DUCKLAKE_EXTENSION_DIR") {
        config["extension_directory"] = json!(extension_directory);
    }
    config
}

/// Two engines, one PostgreSQL catalog: both publish a step carrying the same
/// content, and the catalog ends up with that content exactly once.
///
/// This is the configuration the Python bridge could only approximate with two
/// cursors in one process: each engine here owns its own DuckDB instance, its
/// own transaction and its own guard-row attempt against the same Postgres
/// database. The step numbers collide, so whichever engine commits second must
/// recognise the step as already applied instead of applying it again.
///
/// A reader is then run over the same catalog, which also covers the one thing
/// §13 could not: a reader and writers sharing a catalog that is not file-backed.
#[engine_test(services(Postgres))]
pub fn two_engines_publishing_the_same_step_converge_on_one_outcome(
    engine: &dyn PipelineTestEngine,
) {
    let data = TempDir::new().expect("a temp directory");
    let Some(dsn) = postgres_catalog("ducklake_two_engines") else {
        eprintln!(
            "skipping: no PostgreSQL server answers at {}",
            Service::Postgres.address()
        );
        return;
    };

    let writer = |table: &str| {
        serde_json::from_value::<DuckLakeWriterConfig>(postgres_writer_config(&dsn, &data, table))
            .expect("a writer config")
    };
    let mut catalog = Catalog::open(&writer("orders")).expect("attach the catalog");
    create_table(&mut catalog, "orders");
    drop(catalog);

    let rows = json!([
        {"insert": {"id": 1, "b": true, "i": 10, "s": "a"}},
        {"insert": {"id": 2, "b": false, "i": null, "s": "b"}},
    ])
    .to_string();

    let mut runs = Vec::new();
    for _ in 0..2 {
        let input = tempfile::NamedTempFile::new().expect("a temp file");
        input
            .as_file()
            .write_all(rows.as_bytes())
            .expect("write the input");
        let config = serde_json::from_value(json!({
            "name": "ducklake_two_engines",
            "workers": 1,
            "inputs": {
                "rows": {
                    "paused": true,
                    "stream": "test_input1",
                    "transport": { "name": "file_input", "config": { "path": input.path() } },
                    "format": {
                        "name": "json",
                        "config": { "update_format": "insert_delete", "array": true }
                    }
                }
            },
            "outputs": {
                "test_output1": {
                    "stream": "test_output1",
                    "transport": {
                        "name": "ducklake_output",
                        "config": postgres_writer_config(&dsn, &data, "orders")
                    }
                }
            }
        }))
        .expect("a pipeline config");

        let (error_sender, error_receiver) = crossbeam::channel::unbounded();
        let controller = engine
            .controller(
                &TestCircuit::identity(relation()),
                &config,
                Box::new(move |error, _| {
                    let _ = error_sender.send(format!("ducklake_two_engines: {error}"));
                }),
            )
            .expect("start the controller");
        controller.start();
        controller
            .start_input_endpoint("rows")
            .expect("start the input endpoint");
        runs.push((controller, error_receiver, input));
    }

    for (controller, errors, _input) in &runs {
        wait(
            || controller.status().num_total_processed_records() >= 2 || !errors.is_empty(),
            60_000,
        )
        .expect("both engines to process the step");
    }
    for (controller, errors, _input) in runs {
        controller.stop().expect("stop the controller");
        assert!(
            errors.is_empty(),
            "the connector failed: {:?}",
            errors.try_iter().collect::<Vec<_>>()
        );
    }

    // Prove the outcome with a reader over the same catalog rather than by
    // counting rows in it: the reader is the other half of the protocol, and it
    // sees the catalog the way a pipeline does.
    let output = tempfile::NamedTempFile::new().expect("a temp file");
    let reader = json!({
        "catalog": dsn,
        "table": "orders",
        "poll_interval": "50ms",
    });
    let mut reader = reader.as_object().expect("an object").clone();
    if let Ok(extension_directory) = std::env::var("DUCKLAKE_EXTENSION_DIR") {
        reader.insert("extension_directory".to_owned(), json!(extension_directory));
    }
    let config = serde_json::from_value(json!({
        "name": "ducklake_two_engines_reader",
        "workers": 1,
        "inputs": {
            "rows": {
                "stream": "test_input1",
                "transport": { "name": "ducklake_input", "config": serde_json::Value::Object(reader) }
            }
        },
        "outputs": {
            "test_output1": {
                "stream": "test_output1",
                "transport": { "name": "file_output", "config": { "path": output.path() } },
                "format": { "name": "json", "config": { "update_format": "insert_delete" } }
            }
        }
    }))
    .expect("a pipeline config");

    let (error_sender, error_receiver) = crossbeam::channel::unbounded();
    let controller = engine
        .controller(
            &TestCircuit::identity(relation()),
            &config,
            Box::new(move |error, _| {
                let _ = error_sender.send(format!("ducklake_two_engines_reader: {error}"));
            }),
        )
        .expect("start the controller");
    controller.start();
    controller
        .start_input_endpoint("rows")
        .expect("start the input endpoint");
    wait(
        || controller.status().num_total_processed_records() >= 2 || !error_receiver.is_empty(),
        60_000,
    )
    .expect("the reader to bootstrap the published content");
    assert!(
        error_receiver.is_empty(),
        "the connector failed: {:?}",
        error_receiver.try_iter().collect::<Vec<_>>()
    );
    controller.stop().expect("stop the controller");

    let expected = Updates::from_json(
        published_schema(&relation()),
        &[
            (1, r#"{"id": 1, "b": true, "i": 10, "s": "a"}"#),
            (1, r#"{"id": 2, "b": false, "i": null, "s": "b"}"#),
        ],
    )
    .expect("expected rows");
    let actual = read::insert_delete_updates(&relation(), &std::fs::read(output.path()).unwrap())
        .expect("parse the output file");
    assert_eq!(
        actual, expected,
        "the content both engines published is in the catalog exactly once"
    );
}
