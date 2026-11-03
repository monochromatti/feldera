//! Integration tests against a real DuckLake catalog.
//!
//! These are the tier that a unit test cannot reach: DuckLake's own transaction
//! log, its guard-row conflicts, and the snapshot the published batch becomes.
//! They are skipped — not failed — when the environment is absent, so a plain
//! `cargo test` in CI (which has no DuckDB) stays green:
//!
//! ```text
//! DUCKLAKE_DUCKDB_LIB=.../lib/libduckdb.dylib
//! DUCKLAKE_EXTENSION_DIR=.../v2.0.0/<platform>/
//! ```
//!
//! Both are exported by the `devenv shell` of the connector's specification repo
//! (`nix/ducklake-2.0.nix`, recorded in its `TODO.md` §12). The extension is read
//! straight out of the read-only store directory because only `INSTALL`, never
//! `LOAD`, writes next to it.
//!
//! Each test gets its own catalog under a per-process temp directory: a
//! file-backed DuckLake catalog admits one process, so sharing one between tests
//! would make them order-dependent rather than independent.

use std::collections::BTreeMap;

use feldera_types::transport::ducklake::{
    DEFAULT_ALIAS, DEFAULT_GUARD_TABLE, DEFAULT_SCHEMA, DuckLakeCatalogConfig, DuckLakeSinkConfig,
    DuckLakeWriterConfig,
};
use serde_json::json;

use super::catalog::Catalog;
use super::protocol::{Batch, Decision, Delta, Op};
use super::reader::{ReaderCatalog, ReaderState, Table, advance};

/// A writer config pointing at a fresh catalog, or `None` when the integration
/// environment is not set up.
fn live_config(name: &str) -> Option<DuckLakeWriterConfig> {
    if std::env::var_os("DUCKLAKE_DUCKDB_LIB").is_none()
        || std::env::var_os("DUCKLAKE_EXTENSION_DIR").is_none()
    {
        return None;
    }
    let root = std::env::temp_dir().join(format!("fd-ducklake-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(root.join("data")).expect("create the catalog directory");
    Some(DuckLakeWriterConfig {
        catalog: DuckLakeCatalogConfig {
            catalog: format!("ducklake:{}/meta.ducklake", root.display()),
            schema: DEFAULT_SCHEMA.to_owned(),
            alias: DEFAULT_ALIAS.to_owned(),
            data_path: Some(root.join("data").display().to_string()),
            extension_directory: std::env::var("DUCKLAKE_EXTENSION_DIR").ok(),
            inlining_row_limit: None,
        },
        output_group: "orders".to_owned(),
        generation: "gen-1".to_owned(),
        guard_table: DEFAULT_GUARD_TABLE.to_owned(),
        tables: BTreeMap::from([(
            "orders_out".to_owned(),
            DuckLakeSinkConfig {
                table: "orders".to_owned(),
                key_columns: vec![],
            },
        )]),
        epoch: None,
        send_snapshot: true,
    })
}

/// The batch a step would produce: two rows, inserted into an empty table.
fn batch(catalog: &Catalog) -> Batch {
    Batch {
        output_group: "orders".to_owned(),
        generation: "gen-1".to_owned(),
        epoch: catalog.epoch(),
        base: 0,
        end: 1,
        resync: true,
        deltas: vec![
            Delta::new("orders", Op::Insert, json!({"id": 1, "name": "a"})),
            Delta::new("orders", Op::Insert, json!({"id": 2, "name": "b"})),
        ],
    }
}

/// Republishing a batch must not apply it twice, and must not be reported as a
/// refusal: the guard row is what makes a retry a no-op rather than a duplicate.
///
/// This is the `publish_idempotent` obligation in its real setting — the one a
/// retry after a crash actually exercises, where the first attempt may or may
/// not have committed.
#[test]
fn publishing_the_same_batch_twice_applies_it_once() {
    let Some(config) = live_config("idempotent") else {
        return;
    };
    let mut catalog = Catalog::open(&config).expect("attach a fresh catalog");
    catalog
        .execute_sql("CREATE TABLE dl.orders (id INT, name VARCHAR)")
        .expect("create the sink table");

    let batch = batch(&catalog);
    let first = catalog.publish(&batch, 8).expect("first publication");
    assert_eq!(first.decision, Decision::Apply);
    assert_eq!(first.attempts, 1);

    let again = catalog.publish(&batch, 8).expect("republication");
    assert_eq!(again.decision, Decision::Duplicate);

    assert_eq!(catalog.frontier(), 1, "the frontier is the batch's end");
    assert_eq!(
        catalog
            .scalar("SELECT count(*) FROM dl.orders")
            .unwrap()
            .as_deref(),
        Some("2"),
        "the rows were applied exactly once"
    );
}

/// The frontier survives the connection: reopening the catalog must not lose the
/// record that step 1 was published, or a restart would re-apply it.
#[test]
fn the_frontier_survives_reopening_the_catalog() {
    let Some(config) = live_config("reopen") else {
        return;
    };
    {
        let mut catalog = Catalog::open(&config).expect("attach a fresh catalog");
        catalog
            .execute_sql("CREATE TABLE dl.orders (id INT, name VARCHAR)")
            .expect("create the sink table");
        catalog.publish(&batch(&catalog), 8).expect("publication");
    }

    let mut catalog = Catalog::open(&config).expect("reattach the catalog");
    assert_eq!(catalog.frontier(), 1);

    // The same batch, recognised after a restart as already applied.
    let replay = batch(&catalog);
    let result = catalog.publish(&replay, 8).expect("replay");
    assert_eq!(result.decision, Decision::Duplicate);
    assert_eq!(
        catalog
            .scalar("SELECT count(*) FROM dl.orders")
            .unwrap()
            .as_deref(),
        Some("2"),
        "a replay after restart applied nothing"
    );
}

/// The projection the reader builds from a relation, for the two-column test
/// table.  Spelled out here so the test fails if the reader's projection stops
/// matching what `to_json` needs.
const PROJECTION: &str = r#"{'id': "id", 'name': "name"}"#;

/// Run one read attempt and collect what it handed on.
fn advance_once(
    config: &DuckLakeWriterConfig,
    state: ReaderState,
) -> (Vec<(String, String)>, ReaderState) {
    let catalog = ReaderCatalog::attach(&config.catalog).expect("attach the catalog read-only");
    let table = Table::new(&config.catalog, "main", "orders", PROJECTION).expect("a table address");
    let mut pushed = Vec::new();
    let next = {
        let mut sink = |change_type: &str, json: &str| {
            pushed.push((change_type.to_owned(), json.to_owned()));
            Ok(())
        };
        advance(&catalog, &table, &mut sink, state).expect("one read attempt")
    };
    (pushed, next)
}

/// Commit `sql` through a writer connection, then let go of the catalog so the
/// reader can attach to it (a file catalog admits one process).
fn seed(config: &DuckLakeWriterConfig, sql: &str) {
    let mut catalog = Catalog::open(config).expect("attach the catalog for writing");
    catalog.execute_sql(sql).expect("seed the catalog");
}

/// The bootstrap read returns the table and remembers the snapshot it read, so
/// the next attempt does not read it again — the failure this prevents is a
/// restart that re-reads the whole table and doubles every count.
#[test]
fn bootstrap_reads_the_table_once_and_records_its_snapshot() {
    let Some(config) = live_config("bootstrap") else {
        return;
    };
    seed(
        &config,
        "CREATE TABLE dl.main.orders (id INT, name VARCHAR); \
         INSERT INTO dl.main.orders VALUES (1, 'a'), (2, 'b')",
    );

    let (pushed, state) = advance_once(&config, ReaderState::default());
    assert!(state.bootstrapped, "the first attempt bootstraps");
    assert!(
        state.snapshot >= 2,
        "the snapshot of the last commit is recorded, got {}",
        state.snapshot
    );
    assert_eq!(
        pushed
            .iter()
            .map(|(_, json)| json.as_str())
            .collect::<Vec<_>>(),
        vec![r#"{"id":1,"name":"a"}"#, r#"{"id":2,"name":"b"}"#],
        "both rows, as JSON"
    );

    let (again, state_again) = advance_once(&config, state);
    assert!(
        again.is_empty(),
        "nothing new happened, so nothing is replayed"
    );
    assert_eq!(state_again, state, "the snapshot does not move on its own");
}

/// A commit after the bootstrap is replayed with one weight per row, and the
/// window is not read twice.  An update arrives as a preimage/postimage pair —
/// the writer's reason for emitting updates rather than delete+insert.
#[test]
fn a_new_snapshot_is_replayed_with_its_change_types() {
    let Some(config) = live_config("changes") else {
        return;
    };
    seed(
        &config,
        "CREATE TABLE dl.main.orders (id INT, name VARCHAR); \
         INSERT INTO dl.main.orders VALUES (1, 'a'), (2, 'b')",
    );
    let (_, state) = advance_once(&config, ReaderState::default());

    seed(
        &config,
        "BEGIN; UPDATE dl.main.orders SET name = 'a2' WHERE id = 1; \
         DELETE FROM dl.main.orders WHERE id = 2; \
         INSERT INTO dl.main.orders VALUES (3, 'c'); COMMIT",
    );

    let (pushed, state) = advance_once(&config, state);
    let mut got = pushed.clone();
    got.sort();
    assert_eq!(
        got,
        vec![
            ("delete".to_owned(), r#"{"id":2,"name":"b"}"#.to_owned()),
            ("insert".to_owned(), r#"{"id":3,"name":"c"}"#.to_owned()),
            (
                "update_postimage".to_owned(),
                r#"{"id":1,"name":"a2"}"#.to_owned()
            ),
            (
                "update_preimage".to_owned(),
                r#"{"id":1,"name":"a"}"#.to_owned()
            ),
        ],
        "one row per change, with the change type the weight comes from"
    );

    let (again, state_again) = advance_once(&config, state);
    assert!(again.is_empty(), "the applied window is not replayed");
    assert_eq!(state_again, state);
}
