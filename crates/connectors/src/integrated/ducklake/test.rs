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
use feldera_types::{
    program_schema::Relation, transport::ducklake::DuckLakeWriterConfig,
};
use serde_json::json;
use tempfile::TempDir;

use super::catalog::Catalog;
use crate::test::{
    engine_test, published_schema, read,
    pipeline::{PipelineTestEngine, TestCircuit},
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
    assert_eq!(
        catalog.frontier(),
        1,
        "the guard row records the step it published"
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
