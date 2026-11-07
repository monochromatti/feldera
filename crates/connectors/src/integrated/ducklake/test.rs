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
    } else {
        // Refused rather than defaulted: without it the connector would fall back
        // to `INSTALL ducklake`, so the case would either reach the network or
        // fail as though the connector were broken — neither is what a test that
        // forgot its environment should report.
        panic!(
            "DUCKLAKE_EXTENSION_DIR is not set; the engine tests need the vendored extension so DuckDB never reaches the network"
        );
    }
    config
}

/// The `extension_directory` a case should configure.
///
/// DuckDB writes into that directory only when it has to `INSTALL` something, and
/// a PostgreSQL catalog makes DuckLake load `postgres_scanner` — the one
/// extension this connector needs beyond `ducklake` itself.  Two cases, and both
/// are load-bearing:
///
/// - the vendored directory already ships `postgres_scanner` (the derivation
///   vendors both), so it is used **as is**: a read-only directory suffices, and
///   running against the read-only path is what keeps that a fact rather than a
///   claim (verified with DuckDB 2.0.0: `ATTACH` succeeds with the store path);
/// - it does not, so the directory is copied somewhere writable, because the
///   `ATTACH` would otherwise fail with `Cannot open file
///   "…/postgres_scanner.duckdb_extension.tmp-…": Permission denied` (measured).
fn extension_dir(dir: &TempDir) -> Option<String> {
    let source = std::env::var("DUCKLAKE_EXTENSION_DIR").ok()?;
    if has_extension(std::path::Path::new(&source), "postgres_scanner") {
        return Some(source);
    }
    let target = dir.path().join("extensions");
    copy_tree(std::path::Path::new(&source), &target).ok()?;
    Some(target.display().to_string())
}

/// Whether `dir` holds a `name` extension anywhere below it.
fn has_extension(dir: &std::path::Path, name: &str) -> bool {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return false;
    };
    entries.flatten().any(|entry| {
        let path = entry.path();
        if path.is_dir() {
            return has_extension(&path, name);
        }
        path.file_name()
            .and_then(|file| file.to_str())
            .is_some_and(|file| file.starts_with(name) && file.ends_with(".duckdb_extension"))
    })
}

fn copy_tree(source: &std::path::Path, target: &std::path::Path) -> std::io::Result<()> {
    std::fs::create_dir_all(target)?;
    for entry in std::fs::read_dir(source)? {
        let entry = entry?;
        let destination = target.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_tree(&entry.path(), &destination)?;
        } else {
            std::fs::copy(entry.path(), destination)?;
        }
    }
    Ok(())
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
    // Read the published state while the pipeline is still running, then stop it.
    // Two things make a naive read wrong.  Processing and publishing are different
    // events — the circuit emits, the writer commits afterwards on its own thread —
    // so the read must poll.  And a DuckLake attachment only sees the snapshot it
    // attached with: a commit made by another attachment is invisible to it, so a
    // poll has to re-attach each time (measured: a table created by a second
    // attachment does not even appear in a long-lived one).  Polling after
    // `stop()` would test shutdown instead of the protocol, so the controller
    // stays alive until the rows are there.
    wait(
        || {
            Catalog::open(&writer(&dir, "orders"))
                .and_then(|mut catalog| catalog.scalar("SELECT count(*) FROM dl.main.orders"))
                .is_ok_and(|count| count.as_deref() == Some("2"))
        },
        60_000,
    )
    .expect("the writer to publish the step");
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
    let frontier = catalog.frontier().expect("read the frontier");
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

/// One pipeline reading one catalog and writing another.
///
/// This is the composition the two cases above cannot show: a DuckLake source and
/// a DuckLake sink in one pipeline, each owning its own attachment.  Two catalogs
/// rather than one because a file-backed catalog admits a single attachment — a
/// reader and a writer of the same file catalog fence each other off, which is
/// exactly why the same-catalog case needs PostgreSQL and lives in
/// `two_engines_publishing_the_same_step_converge_on_one_outcome`.
#[engine_test]
pub fn ducklake_to_ducklake_copies_a_table_into_another(engine: &dyn PipelineTestEngine) {
    let source_dir = TempDir::new().expect("a temp directory");
    let sink_dir = TempDir::new().expect("a temp directory");

    let mut source =
        Catalog::open(&writer(&source_dir, "orders")).expect("attach the source catalog");
    create_table(&mut source, "orders");
    source
        .execute_sql("INSERT INTO dl.main.orders VALUES (1, true, 10, 'a'), (2, false, NULL, 'b')")
        .expect("seed the source table");
    drop(source);

    let mut sink =
        Catalog::open(&writer(&sink_dir, "orders_out")).expect("attach the sink catalog");
    create_table(&mut sink, "orders_out");
    drop(sink);

    let mut reader = catalog_config(&source_dir);
    reader["table"] = json!("orders");
    reader["poll_interval"] = json!("50ms");
    let config = serde_json::from_value(json!({
        "name": "ducklake_both",
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
                "transport": {
                    "name": "ducklake_output",
                    "config": writer_config(&sink_dir, "orders_out"),
                },
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
                let _ = error_sender.send(format!("ducklake_both suite: {error}"));
            }),
        )
        .expect("start the controller");
    controller.start();
    controller
        .start_input_endpoint("rows")
        .expect("start the input endpoint");

    wait(
        || controller.status().num_total_processed_records() == 2 || !error_receiver.is_empty(),
        60_000,
    )
    .expect("the source table to be read");
    assert!(
        error_receiver.is_empty(),
        "the connector failed: {:?}",
        error_receiver.try_iter().collect::<Vec<_>>()
    );
    // Poll a *fresh* attachment: a DuckLake attachment only sees the snapshot it
    // attached with, so re-attaching is what lets one attachment observe another's
    // commit.  The controller stays alive until the rows are there — reading after
    // `stop()` would test shutdown rather than the protocol.
    wait(
        || {
            Catalog::open(&writer(&sink_dir, "orders_out"))
                .and_then(|mut catalog| {
                    catalog.rows("SELECT id, b, i, s FROM dl.main.orders_out ORDER BY id")
                })
                .is_ok_and(|rows| rows.len() == 2)
        },
        60_000,
    )
    .expect("the sink to publish the step");
    let mut verify =
        Catalog::open(&writer(&sink_dir, "orders_out")).expect("reattach the sink catalog");
    assert_eq!(
        verify
            .rows("SELECT id, b, i, s FROM dl.main.orders_out ORDER BY id")
            .expect("read the sink table"),
        ["1|true|10|a", "2|false|NULL|b"],
        "the sink table holds what the source table held"
    );
    assert!(
        verify.frontier().expect("read the frontier") >= 1,
        "the sink recorded the step it published"
    );
    controller.stop().expect("stop the controller");
}

/// A commit made by another *process* arrives in the pipeline.
///
/// Every other case here commits through this process's own DuckDB.  A
/// deployment does not: the writer that changes the catalog is usually someone
/// else's process, which is why the reader re-attaches on every poll instead of
/// trusting the attachment it opened.  The case runs the `duckdb` CLI as that
/// other process — a real second DuckDB, with its own transaction — and expects
/// the pipeline to follow it.
///
/// The CLI is looked up in `DUCKLAKE_DUCKDB_CLI` and then on `PATH`; without it
/// the case skips, because DuckDB in-process cannot stand in for a second
/// process.
#[engine_test(services(Postgres))]
pub fn ducklake_input_follows_a_commit_from_another_process(engine: &dyn PipelineTestEngine) {
    let Some(cli) = duckdb_cli() else {
        eprintln!("skipping: no duckdb CLI (set DUCKLAKE_DUCKDB_CLI or put it on PATH)");
        return;
    };
    let data = TempDir::new().expect("a temp directory");
    let Some(dsn) = postgres_catalog("ducklake_cross_process") else {
        eprintln!(
            "skipping: no PostgreSQL server answers at {}",
            Service::Postgres.address()
        );
        return;
    };
    let Some(extensions) = extension_dir(&data) else {
        eprintln!("skipping: DUCKLAKE_EXTENSION_DIR is not set");
        return;
    };

    let mut provision = Catalog::open(
        &serde_json::from_value::<DuckLakeWriterConfig>(postgres_writer_config(
            &dsn, &data, "orders",
        ))
        .expect("a writer config"),
    )
    .expect("attach the catalog");
    create_table(&mut provision, "orders");
    provision
        .execute_sql("INSERT INTO dl.main.orders VALUES (1, true, 10, 'a')")
        .expect("seed the table");
    drop(provision);

    let output = tempfile::NamedTempFile::new().expect("a temp file");
    let config = serde_json::from_value(json!({
        "name": "ducklake_cross_process",
        "workers": 1,
        "inputs": {
            "rows": {
                "stream": "test_input1",
                "transport": {
                    "name": "ducklake_input",
                    "config": {
                        "catalog": dsn,
                        "data_path": format!("{}/data", data.path().display()),
                        "table": "orders",
                        "poll_interval": "50ms",
                        "extension_directory": extensions,
                    }
                },
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
                let _ = error_sender.send(format!("ducklake_cross_process suite: {error}"));
            }),
        )
        .expect("start the controller");
    controller.start();
    controller
        .start_input_endpoint("rows")
        .expect("start the input endpoint");
    wait(
        || controller.status().num_total_processed_records() == 1 || !error_receiver.is_empty(),
        60_000,
    )
    .expect("the bootstrap to be processed");

    // The other process: a second DuckDB, with its own transaction, committing a
    // row into the same catalog while the pipeline runs.
    let script = format!(
        "SET extension_directories = ['{extensions}'];\
         LOAD ducklake;\
         ATTACH '{dsn}' AS dl (DATA_PATH '{}');\
         INSERT INTO dl.main.orders VALUES (2, false, 20, 'b');",
        data.path().join("data").display(),
    );
    let status = std::process::Command::new(&cli)
        .arg("-c")
        .arg(&script)
        .status()
        .expect("run the duckdb CLI");
    assert!(status.success(), "the external commit failed: {status}");

    wait(
        || controller.status().num_total_processed_records() >= 2 || !error_receiver.is_empty(),
        60_000,
    )
    .expect("the external commit to be followed");
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
            (1, r#"{"id": 2, "b": false, "i": 20, "s": "b"}"#),
        ],
    )
    .expect("expected rows");
    let actual = read::insert_delete_updates(&relation(), &std::fs::read(output.path()).unwrap())
        .expect("parse the output file");
    assert_eq!(actual, expected, "the row another process committed");
}

/// The `duckdb` CLI that matches the vendored extension, if there is one.
///
/// A CLI is the only way this suite can produce a commit from a second process, and
/// it has to be the same DuckDB version as the extension that will be loaded into
/// it: a `1.5.2` CLI on `PATH` beside the vendored `2.0.0` extension is a
/// confusing failure rather than a skip, so the version is checked rather than
/// assumed.
fn duckdb_cli() -> Option<String> {
    let mut candidates: Vec<std::path::PathBuf> = Vec::new();
    if let Ok(path) = std::env::var("DUCKLAKE_DUCKDB_CLI") {
        candidates.push(path.into());
    }
    if let Some(path) = std::env::var_os("PATH") {
        candidates.extend(std::env::split_paths(&path).map(|dir| dir.join("duckdb")));
    }
    candidates
        .into_iter()
        .find(|candidate| {
            candidate.is_file()
                && std::process::Command::new(candidate)
                    .arg("--version")
                    .output()
                    .map(|out| {
                        // Parsed, not substring-matched: `2.0.1` and a banner that
                        // merely mentions the version must both be accepted, and
                        // a `1.5.2` beside a `2.0` extension must not be.
                        let version = String::from_utf8_lossy(&out.stdout);
                        let major = version
                            .split_whitespace()
                            .find_map(|word| word.strip_prefix('v'))
                            .and_then(|word| word.split('.').next())
                            .and_then(|major| major.parse::<u32>().ok());
                        out.status.success() && major == Some(2)
                    })
                    .unwrap_or(false)
        })
        .map(|candidate| candidate.display().to_string())
}

/// A catalog in a fresh database on the `Postgres` service, as an `ATTACH`
/// string.
///
/// DuckLake does not create the database it stores its metadata in (measured:
/// `Unable to connect to Postgres … database "dl_probe" does not exist`), so the
/// case creates one through a plain `postgres` connection, exactly as a
/// deployment would.
pub(super) fn postgres_catalog(prefix: &str) -> Option<String> {
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
    if let Some(extension_directory) = extension_dir(data) {
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
    if let Some(extension_directory) = extension_dir(&data) {
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
