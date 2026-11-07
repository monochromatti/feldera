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
//!
//! Two cases at the end (`two_groups_commit_concurrently_into_one_catalog`,
//! `a_resync_clears_only_its_own_group`) use a PostgreSQL catalog instead, in a
//! database `crate::test::postgres_catalog` creates for them.  That is not a
//! preference: a file catalog serialises writers, so a *conflicting* commit — the
//! only thing that exercises the retry path — cannot be produced against one.
//! They skip themselves with a printed reason when no server answers.

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
        // Said out loud, and once per case: a bare `return` is indistinguishable
        // from a pass in the harness output, so a suite that silently ran nothing
        // reads as a suite that passed.
        eprintln!(
            "[skipped] {name}: set DUCKLAKE_DUCKDB_LIB and DUCKLAKE_EXTENSION_DIR to run the live cases"
        );
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

/// A batch that *appends* rather than replacing the target's content.
///
/// The idempotency tests use this rather than a resync on purpose: reapplying a
/// resync yields the same rows as applying it once, so a row count cannot tell
/// `Duplicate` from `Apply` twice.  An append that is applied twice doubles the
/// rows, which is the corruption the guard row exists to prevent.
fn batch(catalog: &Catalog) -> Batch {
    Batch {
        output_group: "orders".to_owned(),
        generation: "gen-1".to_owned(),
        epoch: catalog.epoch(),
        base: 0,
        end: 1,
        resync: false,
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
    let first = catalog.publish(&batch, None, 8).expect("first publication");
    assert_eq!(first.decision, Decision::Apply);
    assert_eq!(first.attempts, 1);

    let again = catalog.publish(&batch, None, 8).expect("republication");
    assert_eq!(again.decision, Decision::Duplicate);

    assert_eq!(
        catalog.frontier().expect("read the frontier"),
        1,
        "the frontier is the batch's end"
    );
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
        catalog
            .publish(&batch(&catalog), None, 8)
            .expect("publication");
    }

    let mut catalog = Catalog::open(&config).expect("reattach the catalog");
    assert_eq!(catalog.frontier().expect("read the frontier"), 1);

    // The same batch, recognised after a restart as already applied.
    let replay = batch(&catalog);
    let result = catalog.publish(&replay, None, 8).expect("replay");
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

/// The batch that follows another one: same group and run, no resync.
fn step(catalog: &Catalog, base: u64, end: u64, deltas: Vec<Delta>) -> Batch {
    Batch {
        base,
        end,
        resync: false,
        deltas,
        ..batch(catalog)
    }
}

/// The catalog's committed snapshot, read the way the connector reads it.
fn snapshot(catalog: &mut Catalog) -> u64 {
    catalog
        .scalar("SELECT * FROM ducklake_current_snapshot('dl')")
        .expect("read the current snapshot")
        .expect("a snapshot number")
        .parse()
        .expect("a snapshot number")
}

/// A payload that fails must take the guard update down with it.
///
/// The failure this prevents is the one the protocol exists for: a guard that
/// records step 2 while the payload of step 2 rolled back would turn every retry
/// into a `DUPLICATE` and lose that step's output for good.  The snapshot count
/// is pinned too, because one publication must be one snapshot — that is what
/// lets a reader see the payload and the guard together.
#[test]
fn a_payload_failure_leaves_the_guard_and_the_history_where_they_were() {
    let Some(config) = live_config("atomic") else {
        return;
    };
    let mut catalog = Catalog::open(&config).expect("attach a fresh catalog");
    catalog
        .execute_sql("CREATE TABLE dl.orders (id INT, name VARCHAR NOT NULL)")
        .expect("create the sink table");
    let first = batch(&catalog);
    catalog
        .publish(&first, None, 8)
        .expect("the first publication");
    let committed = snapshot(&mut catalog);

    let bad = step(
        &catalog,
        1,
        2,
        vec![Delta::new(
            "orders",
            Op::Insert,
            json!({"id": 3, "name": null}),
        )],
    );
    let error = catalog
        .publish(&bad, None, 1)
        .expect_err("a NOT NULL violation must fail the publication");
    assert!(
        format!("{error:#}").contains("NOT NULL"),
        "unexpected error: {error:#}"
    );

    assert_eq!(
        catalog.frontier().expect("read the frontier"),
        1,
        "the guard moved for a failed payload"
    );
    assert_eq!(
        catalog
            .rows("SELECT id, name FROM dl.orders ORDER BY id")
            .unwrap(),
        ["1|a", "2|b"],
        "a failed publication changed the payload"
    );
    assert_eq!(
        snapshot(&mut catalog),
        committed,
        "a failed publication left a snapshot behind"
    );
}

/// A payload that diverges from the committed one is refused, not mistaken for a
/// retry.
///
/// The identity of a batch is its generation and interval; the digest is what
/// tells a replay from a lie.  Treating a divergent payload as a duplicate would
/// keep the wrong rows and call the divergence a success.
#[test]
fn a_divergent_payload_for_a_committed_identity_is_refused() {
    let Some(config) = live_config("divergent") else {
        return;
    };
    let mut catalog = Catalog::open(&config).expect("attach a fresh catalog");
    catalog
        .execute_sql("CREATE TABLE dl.orders (id INT, name VARCHAR)")
        .expect("create the sink table");
    let first = batch(&catalog);
    catalog
        .publish(&first, None, 8)
        .expect("the first publication");

    // Same identity — generation and interval — with different content.
    let mut divergent = batch(&catalog);
    divergent.deltas = vec![Delta::new(
        "orders",
        Op::Insert,
        json!({"id": 1, "name": "zzz"}),
    )];
    let error = catalog
        .publish(&divergent, None, 1)
        .expect_err("a divergent payload is not a duplicate");
    assert!(
        format!("{error:#}").contains("different content"),
        "unexpected error: {error:#}"
    );

    assert_eq!(
        catalog
            .rows("SELECT id, name FROM dl.orders ORDER BY id")
            .unwrap(),
        ["1|a", "2|b"],
        "the refused payload was applied anyway"
    );
    assert_eq!(catalog.frontier().expect("read the frontier"), 1);
}

/// A resync replaces the whole content, and replaying it is still a duplicate.
///
/// This is the only case where a batch may apply from a frontier it does not sit
/// on: a resync is a statement about the target's *content*, not a step in the
/// sequence.  It must not become a way to apply an old step twice.
#[test]
fn a_resync_replaces_the_content_and_replays_as_a_duplicate() {
    let Some(config) = live_config("resync") else {
        return;
    };
    let mut catalog = Catalog::open(&config).expect("attach a fresh catalog");
    catalog
        .execute_sql("CREATE TABLE dl.orders (id INT, name VARCHAR)")
        .expect("create the sink table");
    let first = batch(&catalog);
    catalog
        .publish(&first, None, 8)
        .expect("the first publication");

    // Row 1 disappears, row 2 is rewritten: the batch is the whole content.
    let resync = Batch {
        base: 0,
        end: 5,
        // Explicit: this case is *about* resync, and it used to inherit the flag
        // from the shared helper — so renaming the helper's default would have
        // silently turned this into an append that the guard classifies `Stale`.
        resync: true,
        deltas: vec![Delta::new(
            "orders",
            Op::Insert,
            json!({"id": 2, "name": "b2"}),
        )],
        ..batch(&catalog)
    };
    let result = catalog.publish(&resync, None, 8).expect("a resync applies");
    assert_eq!(result.decision, Decision::Apply);
    assert_eq!(
        catalog
            .rows("SELECT id, name FROM dl.orders ORDER BY id")
            .unwrap(),
        ["2|b2"],
        "a resync must leave exactly its own content"
    );

    let replay = catalog
        .publish(&resync, None, 8)
        .expect("a replayed resync");
    assert_eq!(replay.decision, Decision::Duplicate);
    assert_eq!(
        catalog
            .rows("SELECT id, name FROM dl.orders ORDER BY id")
            .unwrap(),
        ["2|b2"],
        "a replayed resync applied twice"
    );
}

/// A keyed update replaces by key, leaving only the post-image.
#[test]
fn a_keyed_update_leaves_the_post_image() {
    let Some(mut config) = live_config("keyed") else {
        return;
    };
    for sink in config.tables.values_mut() {
        sink.key_columns = vec!["id".to_owned()];
    }
    let mut catalog = Catalog::open(&config).expect("attach a fresh catalog");
    catalog
        .execute_sql("CREATE TABLE dl.orders (id INT, name VARCHAR)")
        .expect("create the sink table");

    let first = step(
        &catalog,
        0,
        1,
        vec![Delta::new(
            "orders",
            Op::Insert,
            json!({"id": 1, "name": "a"}),
        )],
    );
    catalog
        .publish(&first, None, 8)
        .expect("the first publication");

    let update = step(
        &catalog,
        1,
        2,
        vec![
            Delta::new("orders", Op::Delete, json!({"id": 1, "name": "a"})),
            Delta::new("orders", Op::Insert, json!({"id": 1, "name": "a2"})),
        ],
    );
    catalog.publish(&update, None, 8).expect("the update");
    assert_eq!(
        catalog
            .rows("SELECT id, name FROM dl.orders ORDER BY id")
            .unwrap(),
        ["1|a2"],
        "a keyed update must leave exactly the post-image"
    );
}

/// A NULL key part must be matched, not skipped.
///
/// `=` never matches NULL, so a delete-by-key written with `=` deletes nothing
/// for such a row, and the insert that follows leaves two rows where the
/// pipeline said one — with the guard advanced past both.  The Python reference
/// was fixed for the same reason and is pinned by
/// `guard/keyed-replacement-matches-a-null-key` in `scenarios.py`.
#[test]
fn a_null_key_part_is_matched_not_skipped() {
    let Some(mut config) = live_config("null_key") else {
        return;
    };
    for sink in config.tables.values_mut() {
        sink.key_columns = vec!["id".to_owned()];
    }
    let mut catalog = Catalog::open(&config).expect("attach a fresh catalog");
    catalog
        .execute_sql("CREATE TABLE dl.orders (id INT, name VARCHAR)")
        .expect("create the sink table");

    let first = step(
        &catalog,
        0,
        1,
        vec![Delta::new(
            "orders",
            Op::Insert,
            json!({"id": null, "name": "a"}),
        )],
    );
    catalog
        .publish(&first, None, 8)
        .expect("the first publication");

    let update = step(
        &catalog,
        1,
        2,
        vec![
            Delta::new("orders", Op::Delete, json!({"id": null, "name": "a"})),
            Delta::new("orders", Op::Insert, json!({"id": null, "name": "b"})),
        ],
    );
    catalog.publish(&update, None, 8).expect("the update");
    // Counted through `scalar` rather than `rows`: what a NULL renders as in a
    // pipe-joined row is a detail of the formatter, and the failure this guards
    // against is a duplicate row, not a rendering.
    assert_eq!(
        catalog.scalar("SELECT count(*) FROM dl.orders").unwrap(),
        Some("1".to_owned()),
        "a NULL-keyed update must replace the row, not sit next to it"
    );
    assert_eq!(
        catalog
            .scalar("SELECT name FROM dl.orders WHERE id IS NULL")
            .unwrap(),
        Some("b".to_owned())
    );
}

/// Without key columns the table is a bag: a delete removes one copy.
///
/// Removing every matching row would turn a multiset into a set and lose a row
/// the engine still counts.
#[test]
fn a_multiset_delete_removes_one_copy() {
    let Some(config) = live_config("multiset") else {
        return;
    };
    let mut catalog = Catalog::open(&config).expect("attach a fresh catalog");
    catalog
        .execute_sql("CREATE TABLE dl.orders (id INT, name VARCHAR)")
        .expect("create the sink table");
    let row = json!({"id": 7, "name": "a"});

    let first = step(
        &catalog,
        0,
        1,
        vec![
            Delta::new("orders", Op::Insert, row.clone()),
            Delta::new("orders", Op::Insert, row.clone()),
        ],
    );
    catalog
        .publish(&first, None, 8)
        .expect("two copies of one row");
    assert_eq!(
        catalog.rows("SELECT id, name FROM dl.orders").unwrap(),
        ["7|a", "7|a"]
    );

    let second = step(&catalog, 1, 2, vec![Delta::new("orders", Op::Delete, row)]);
    catalog.publish(&second, None, 8).expect("a single delete");
    assert_eq!(
        catalog.rows("SELECT id, name FROM dl.orders").unwrap(),
        ["7|a"],
        "the delete removed every copy instead of one"
    );
}

/// The extension is loaded from the configured directory, not from the default
/// one and not from the network.
///
/// This is the property an offline deployment needs, and it is asserted through
/// `duckdb_extensions().install_path` rather than through a setting value,
/// because the setting is not observable: writing the deprecated
/// `extension_directory` through `duckdb_create_config` is accepted and honoured
/// but `current_setting('extension_directory')` reports an empty string, and the
/// same is true of `extension_directories`.  A test that asserted on the setting
/// name would pass for the wrong reason or fail for none — so this asserts where
/// the loaded binary came from.
///
/// Move `~/.duckdb/extensions` aside and run this suite to see the difference: the
/// other live tests still pass, which is what proves the directory is consulted at
/// all rather than the home directory silently serving the extension.
#[test]
fn the_extension_is_loaded_from_the_configured_directory() {
    let Some(config) = live_config("extension-directories") else {
        return;
    };
    let expected = config
        .catalog
        .extension_directory
        .clone()
        .expect("the test config sets one");

    let mut catalog = Catalog::open(&config).expect("attach a fresh catalog");
    let path = catalog
        .scalar(
            "SELECT install_path FROM duckdb_extensions() \
             WHERE extension_name = 'ducklake' AND loaded",
        )
        .expect("read the loaded extension")
        .expect("ducklake is loaded");
    assert!(
        path.starts_with(&expected),
        "the ducklake extension was loaded from {path:?}, not from the configured \
         directory {expected:?}"
    );
}

/// The projection the reader builds from a relation, for the two-column test
/// table.  Computed by the reader's own code rather than spelled out here: a
/// hand-written copy would agree with itself about key quoting, which is exactly
/// what the copy is supposed to catch.
fn projection() -> String {
    super::reader::projection(&orders_relation()).expect("the reader's projection")
}

/// Run one read attempt and collect what it handed on.
fn advance_once(
    config: &DuckLakeWriterConfig,
    state: ReaderState,
) -> (Vec<(String, String)>, ReaderState) {
    let catalog = ReaderCatalog::attach(&config.catalog).expect("attach the catalog read-only");
    let projection = projection();
    let table =
        Table::new(&config.catalog, "main", "orders", &projection).expect("a table address");
    let mut pushed = Vec::new();
    let next = {
        let mut sink = |change_type: &str, json: &str| {
            pushed.push((change_type.to_owned(), json.to_owned()));
            Ok(())
        };
        let (next, _pushed) =
            advance(&catalog, &table, &mut sink, state).expect("one read attempt");
        next
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

// ---------------------------------------------------------------------------
// The endpoint loop
// ---------------------------------------------------------------------------
//
// `advance` is tested above with a closure for a sink.  What that cannot reach is
// the state machine around it: `Extend`/`Pause`, the `Resume` metadata a
// checkpoint stores, and `Replay`.  The failure this guards is a restart that
// bootstraps a second time — the whole table arriving twice — which no amount of
// correct `advance` logic would prevent, because the loop would pass a default
// state instead of the replayed one.

use std::{
    hash::Hasher,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use feldera_adapterlib::{
    ConnectorMetadata,
    catalog::{
        ArrowStream, DeCollectionHandle, DeCollectionStream, InputCollectionHandle, RecordFormat,
        StagedBuffers,
    },
    errors::controller::ControllerError,
    format::{BufferSize, InputBuffer, ParseError},
    transport::{InputConsumer, Resume, Watermark},
};
use feldera_types::{
    adapter_stats::ConnectorHealth,
    config::FtModel,
    coordination::Completion,
    program_schema::{ColumnType, Field, Relation, SqlIdentifier},
    serde_with_context::SqlSerdeConfig,
    transport::ducklake::DuckLakeReaderConfig,
};

use super::reader;

/// What the reader asked the collection to buffer: `true` for an insert, `false`
/// for a delete, plus the JSON it handed over.
type Pushed = Arc<Mutex<Vec<(bool, String)>>>;

/// A stand-in for the circuit's JSON deserializer.  The reader's whole job
/// against it is `insert`/`delete` then `flush`, so recording those three is
/// enough, and it keeps this test free of a circuit.
struct RecordingStream {
    pending: Vec<(bool, String)>,
    pushed: Pushed,
}

impl RecordingStream {
    fn new(pushed: Pushed) -> Self {
        Self {
            pending: Vec::new(),
            pushed,
        }
    }

    fn record(&mut self, insert: bool, data: &[u8]) {
        self.pending
            .push((insert, String::from_utf8_lossy(data).into_owned()));
    }
}

impl InputBuffer for RecordingStream {
    fn flush(&mut self) -> anyhow::Result<()> {
        self.pushed.lock().unwrap().append(&mut self.pending);
        Ok(())
    }

    fn len(&self) -> BufferSize {
        BufferSize {
            records: self.pending.len(),
            bytes: 0,
        }
    }

    fn hash(&self, _hasher: &mut dyn Hasher) {
        // Only exactly-once endpoints hash, and this one reports `AtLeastOnce`.
        unreachable!("the DuckLake reader must not ask for a replay hash")
    }

    fn take_some(&mut self, n: usize) -> Option<Box<dyn InputBuffer>> {
        if n == 0 || self.pending.is_empty() {
            return None;
        }
        let tail = self.pending.split_off(n.min(self.pending.len()));
        Some(Box::new(RecordingStream {
            pending: std::mem::replace(&mut self.pending, tail),
            pushed: self.pushed.clone(),
        }))
    }
}

impl DeCollectionStream for RecordingStream {
    fn insert(&mut self, data: &[u8], _metadata: &Option<ConnectorMetadata>) -> anyhow::Result<()> {
        self.record(true, data);
        Ok(())
    }

    fn delete(&mut self, data: &[u8], _metadata: &Option<ConnectorMetadata>) -> anyhow::Result<()> {
        self.record(false, data);
        Ok(())
    }

    fn update(
        &mut self,
        _data: &[u8],
        _metadata: &Option<ConnectorMetadata>,
    ) -> anyhow::Result<()> {
        unreachable!("a Z-set stream has no updates, only inserts and deletes")
    }

    fn reserve(&mut self, reservation: usize) {
        self.pending.reserve(reservation);
    }

    fn truncate(&mut self, len: usize) {
        self.pending.truncate(len);
    }

    fn stage(&self, _buffers: Vec<Box<dyn InputBuffer>>) -> Box<dyn StagedBuffers> {
        unreachable!("the DuckLake reader does not stage parser buffers")
    }

    fn fork(&self) -> Box<dyn DeCollectionStream> {
        Box::new(RecordingStream::new(self.pushed.clone()))
    }
}

/// The handle the controller would hand the endpoint.
struct RecordingHandle {
    pushed: Pushed,
}

impl DeCollectionHandle for RecordingHandle {
    fn configure_deserializer(
        &self,
        record_format: RecordFormat,
    ) -> Result<Box<dyn DeCollectionStream>, ControllerError> {
        assert!(
            matches!(record_format, RecordFormat::Json(_)),
            "the reader hands over JSON, so it must ask for a JSON deserializer"
        );
        Ok(Box::new(RecordingStream::new(self.pushed.clone())))
    }

    fn configure_arrow_deserializer(
        &self,
        _config: SqlSerdeConfig,
    ) -> Result<Box<dyn ArrowStream>, ControllerError> {
        unreachable!("the DuckLake reader reads JSON, not Arrow")
    }

    #[cfg(feature = "with-avro")]
    fn configure_avro_deserializer(
        &self,
    ) -> Result<Box<dyn feldera_adapterlib::catalog::AvroStream>, ControllerError> {
        unreachable!("the DuckLake reader reads JSON, not Avro")
    }

    fn fork(&self) -> Box<dyn DeCollectionHandle> {
        Box::new(RecordingHandle {
            pushed: self.pushed.clone(),
        })
    }
}

/// What the loop told the controller.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Event {
    Extended { records: usize, resume: Option<u64> },
    Replayed,
    Error(String),
    Eoi,
}

/// A consumer that records what it was told, so the test can assert on the
/// `Resume` a checkpoint would store.  `Clone` because `InputConsumer` requires
/// `DynClone`; the clones share one log.
#[derive(Clone, Default)]
struct RecordingConsumer {
    events: Arc<Mutex<Vec<Event>>>,
}

impl RecordingConsumer {
    fn events(&self) -> Vec<Event> {
        self.events.lock().unwrap().clone()
    }

    /// The `Extended` events that carried rows.  Every `Queue` is answered now —
    /// a silent one would stall the controller — so the flushes have to be picked
    /// out of the acknowledgements.
    fn flushes(&self) -> Vec<Event> {
        self.events()
            .into_iter()
            .filter(|event| !matches!(event, Event::Extended { records: 0, .. }))
            .collect()
    }

    fn push(&self, event: Event) {
        self.events.lock().unwrap().push(event);
    }

    /// The snapshot the `n`th flush carried.
    fn flush_snapshot(&self, n: usize) -> u64 {
        match self.flushes().get(n) {
            Some(Event::Extended {
                resume: Some(snapshot),
                ..
            }) => *snapshot,
            other => panic!("flush {n} is not an extended with a snapshot: {other:?}"),
        }
    }

    fn metadata(&self, n: usize) -> serde_json::Value {
        json!({ "snapshot": self.flush_snapshot(n), "bootstrapped": true })
    }
}

impl InputConsumer for RecordingConsumer {
    fn max_batch_size(&self) -> usize {
        usize::MAX
    }

    fn pipeline_fault_tolerance(&self) -> Option<FtModel> {
        Some(FtModel::AtLeastOnce)
    }

    fn parse_errors(&self, _errors: Vec<ParseError>) {}

    fn buffered(&self, _amt: BufferSize) {}

    fn replayed(&self, _amt: BufferSize, _hash: u64) {
        self.push(Event::Replayed);
    }

    fn extended(&self, amt: BufferSize, resume: Option<Resume>, _watermarks: Vec<Watermark>) {
        let snapshot = match resume {
            Some(Resume::Seek { seek }) => seek.get("snapshot").and_then(|s| s.as_u64()),
            // A reader that has not bootstrapped yet has no snapshot to resume
            // from, and says so instead of inventing one.
            Some(Resume::Barrier) => None,
            other => panic!("the reader must resume by metadata only, got {other:?}"),
        };
        self.push(Event::Extended {
            records: amt.records,
            resume: snapshot,
        });
    }

    fn eoi(&self) {
        self.push(Event::Eoi);
    }

    fn request_step(&self) {}

    fn start_transaction(&self, _label: Option<&str>) {}

    fn commit_transaction(&self) {}

    fn completion_watcher(&self) -> Option<tokio::sync::watch::Receiver<Completion>> {
        None
    }

    fn error(&self, _fatal: bool, error: anyhow::Error, _tag: Option<&'static str>) {
        self.push(Event::Error(error.to_string()));
    }

    fn update_connector_health(&self, _health: ConnectorHealth) {}
}

/// The `orders` relation the reader is asked to serve.
fn orders_relation() -> Relation {
    Relation {
        name: SqlIdentifier::from("orders".to_owned()),
        fields: vec![
            Field::new("id".into(), ColumnType::int(false)),
            Field::new("name".into(), ColumnType::varchar(false)),
        ],
        materialized: false,
        properties: Default::default(),
        primary_key: None,
    }
}

/// Queue the reader until `done` holds, the way the controller does: a `Queue`
/// that arrives before the reader has read anything flushes nothing, so sending
/// one is not enough.
fn pump(
    what: &str,
    reader: &dyn feldera_adapterlib::transport::InputReader,
    mut done: impl FnMut() -> bool,
) {
    let deadline = Instant::now() + Duration::from_secs(20);
    while Instant::now() < deadline {
        if done() {
            return;
        }
        reader.request(feldera_adapterlib::transport::InputReaderCommand::Queue {
            checkpoint_requested: false,
        });
        std::thread::sleep(Duration::from_millis(10));
    }
    panic!("timed out waiting for {what}");
}

fn wait_for(what: &str, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(20);
    while Instant::now() < deadline {
        if done() {
            return;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    panic!("timed out waiting for {what}");
}

/// Open a reader over `orders` with a 10ms poll interval, so the test is fast
/// without racing the loop.
fn open_reader(
    config: &DuckLakeWriterConfig,
    consumer: &RecordingConsumer,
    pushed: Pushed,
) -> Box<dyn feldera_adapterlib::transport::InputReader> {
    let relation = orders_relation();
    let handle = InputCollectionHandle::new(
        relation.clone(),
        RecordingHandle {
            pushed: pushed.clone(),
        },
        None,
    );
    let reader_config = DuckLakeReaderConfig {
        catalog: config.catalog.clone(),
        table: "orders".to_owned(),
        poll_interval: Some(serde_json::from_value(json!("10ms")).expect("a poll interval")),
    };
    reader::open(
        "orders",
        reader_config,
        &relation,
        &handle,
        Box::new(consumer.clone()),
        None,
    )
    .expect("open the reader")
}

/// The loop bootstraps once, reports the snapshot as its resume point, replays a
/// later commit, and re-reads from a replayed checkpoint **without** bootstrapping
/// again — the duplicate-table failure a restart would otherwise cause.
#[test]
fn the_loop_bootstraps_once_and_replays_from_the_checkpoint_it_reported() {
    use feldera_adapterlib::transport::InputReaderCommand;

    let Some(config) = live_config("loop") else {
        return;
    };
    seed(
        &config,
        "CREATE TABLE dl.main.orders (id INT, name VARCHAR); \
         INSERT INTO dl.main.orders VALUES (1, 'a'), (2, 'b')",
    );

    let consumed = Pushed::default();
    let consumer = RecordingConsumer::default();
    let reader = open_reader(&config, &consumer, consumed.clone());

    // Bootstrap: `Extend` reads, and the controller's repeated `Queue` flushes.
    reader.request(InputReaderCommand::Extend);
    pump("the bootstrap to be flushed", &*reader, || {
        !consumer.flushes().is_empty()
    });

    let bootstrap_snapshot = consumer.flush_snapshot(0);
    assert_eq!(
        consumer.flushes()[0],
        Event::Extended {
            records: 2,
            resume: Some(bootstrap_snapshot)
        },
        "the whole table, with the snapshot it was read at"
    );
    assert_eq!(
        *consumed.lock().unwrap(),
        vec![
            (true, r#"{"id":1,"name":"a"}"#.to_owned()),
            (true, r#"{"id":2,"name":"b"}"#.to_owned()),
        ],
        "each bootstrap row is an insert"
    );

    // A later commit arrives as the change feed, not as a second snapshot.
    seed(&config, "INSERT INTO dl.main.orders VALUES (3, 'c')");
    reader.request(InputReaderCommand::Extend);
    pump("the new commit to be flushed", &*reader, || {
        consumer.flushes().len() >= 2
    });
    assert_eq!(
        consumer.flushes()[1],
        Event::Extended {
            records: 1,
            resume: Some(consumer.flush_snapshot(1))
        },
        "only the new row"
    );
    assert_eq!(
        consumed.lock().unwrap()[2..],
        [(true, r#"{"id":3,"name":"c"}"#.to_owned())],
        "the commit's row, appended after the bootstrap"
    );

    // An idle `Queue` is still answered: the controller's step waits for the
    // signal, so a reader that says nothing when there is no news deadlocks the
    // pipeline instead of pausing cleanly.  It reports no rows and keeps its
    // resume point.
    let idle = consumer.events().len();
    pump("the idle queue to be answered", &*reader, || {
        consumer.events().len() > idle
    });
    assert_eq!(
        consumer.events()[idle],
        Event::Extended {
            records: 0,
            resume: Some(consumer.flush_snapshot(1)),
        },
        "an idle queue is answered without inventing rows"
    );

    // Replaying the *bootstrap* checkpoint must re-read the change since it, not
    // the whole table: this is what a restart does.
    reader.request(InputReaderCommand::Replay {
        metadata: consumer.metadata(0),
        data: rmpv::Value::Nil,
    });
    wait_for("the replay to be reported", || {
        consumer.events().contains(&Event::Replayed)
    });
    // The replay truncates the buffered rows, not the ones already flushed.
    let before = consumed.lock().unwrap().len();
    assert_eq!(before, 3, "a replay keeps the rows it already flushed");

    reader.request(InputReaderCommand::Extend);
    // Wait for the re-read to be *flushed*, not merely for a queue to be
    // answered: an answered `Queue` is not a row.
    pump("the replayed window to be flushed", &*reader, || {
        consumed.lock().unwrap().len() > before
    });
    assert_eq!(
        consumed.lock().unwrap()[before..],
        [(true, r#"{"id":3,"name":"c"}"#.to_owned())],
        "the replayed checkpoint re-reads the change since it, not the table"
    );

    // `Pause` stops the polling: a commit after it must not arrive.
    reader.request(InputReaderCommand::Pause);
    seed(&config, "INSERT INTO dl.main.orders VALUES (4, 'd')");
    let at_pause = consumer.events().len();
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(
        consumer.events().len(),
        at_pause,
        "nothing is read while paused"
    );

    reader.request(InputReaderCommand::Disconnect);
    wait_for("the reader to close", || reader.is_closed());
    assert!(
        !consumer
            .events()
            .iter()
            .any(|event| matches!(event, Event::Error(_))),
        "the loop reported an error: {:?}",
        consumer.events()
    );
}

/// The writer configuration for one output group over a PostgreSQL catalog.
///
/// Separate from `live_config` because the two differ in the catalog string and
/// nothing else the concurrency cases care about; going through JSON like the
/// engine tests do would add a `DuckLakeWriterConfig` round-trip whose failure
/// mode ("a field the test forgot") is the same one the types already prevent.
///
/// `tables` is a list because the resync case needs a group configured with two
/// tables: a resync's scope is only observable when the configured set is wider
/// than the set the batch carries.
fn postgres_group_config(
    dsn: &str,
    data: &str,
    group: &str,
    tables: &[&str],
) -> DuckLakeWriterConfig {
    DuckLakeWriterConfig {
        catalog: DuckLakeCatalogConfig {
            catalog: dsn.to_owned(),
            schema: DEFAULT_SCHEMA.to_owned(),
            alias: DEFAULT_ALIAS.to_owned(),
            data_path: Some(data.to_owned()),
            extension_directory: std::env::var("DUCKLAKE_EXTENSION_DIR").ok(),
            inlining_row_limit: None,
        },
        output_group: group.to_owned(),
        generation: "gen-1".to_owned(),
        guard_table: DEFAULT_GUARD_TABLE.to_owned(),
        tables: tables
            .iter()
            .map(|table| {
                (
                    (*table).to_owned(),
                    DuckLakeSinkConfig {
                        table: (*table).to_owned(),
                        key_columns: vec![],
                    },
                )
            })
            .collect(),
        epoch: None,
        send_snapshot: true,
    }
}

/// An appended step of `rows` rows, identifying the step by `base`.
fn appended(catalog: &Catalog, group: &str, table: &str, base: u64, rows: u64) -> Batch {
    Batch {
        output_group: group.to_owned(),
        generation: "gen-1".to_owned(),
        epoch: catalog.epoch(),
        base,
        end: base + 1,
        resync: false,
        deltas: (0..rows)
            .map(|i| {
                Delta::new(
                    table,
                    Op::Insert,
                    json!({"id": base * 1000 + i, "name": table}),
                )
            })
            .collect(),
    }
}

/// A workshold under `TempDir` for one case's data files, and the catalog
/// environment the live cases need.  `None` means "skip, and say why".
fn postgres_or_skip(name: &str) -> Option<(String, tempfile::TempDir)> {
    let Some(dsn) = super::test::postgres_catalog(name) else {
        eprintln!(
            "skipping {name}: no PostgreSQL server answers at {}",
            crate::test::Service::Postgres.address()
        );
        return None;
    };
    if std::env::var_os("DUCKLAKE_DUCKDB_LIB").is_none()
        || std::env::var_os("DUCKLAKE_EXTENSION_DIR").is_none()
    {
        eprintln!(
            "[skipped] {name}: set DUCKLAKE_DUCKDB_LIB and DUCKLAKE_EXTENSION_DIR to run the live cases"
        );
        return None;
    }
    let dir = tempfile::TempDir::new().expect("a temp directory");
    std::fs::create_dir_all(dir.path().join("data")).expect("create the data directory");
    Some((dsn, dir))
}

/// Two group threads rendezvous through a channel each, so that a thread that
/// panics fails the case instead of hanging it.
///
/// `std::sync::Barrier` was the obvious choice and is the wrong one: a worker that
/// panics at the rendezvous — or before reaching it — leaves its peer blocked
/// forever, and libtest has no timeout, so a broken test hangs the suite rather
/// than failing it. `recv_timeout` turns that into a failure with a message.
fn rendezvous(
    send: &std::sync::mpsc::SyncSender<()>,
    recv: &std::sync::mpsc::Receiver<()>,
    who: &str,
) {
    send.send(())
        .expect("the rendezvous sender outlives the loop");
    recv.recv_timeout(std::time::Duration::from_secs(60))
        .unwrap_or_else(|_| panic!("{who}: the other group never reached the rendezvous"));
}

/// Two output groups committing into one PostgreSQL catalog at the same time must
/// each see exactly their own rows, and each must apply its step exactly once.
///
/// This is the half of the protocol a file-backed catalog cannot show. The file
/// metadata manager admits one writer at a time, so every case above exercises
/// the guard row but never a *conflict*: the retry path — re-read the guard, find
/// the step uncommitted, re-apply the same batch identity — only runs when
/// DuckLake resolves two concurrent commits against each other. A bug there is
/// invisible to the file suite and shows up here as a lost step (the retry saw a
/// stale guard and answered `Duplicate`) or a doubled one (the retry re-applied a
/// batch that had in fact landed).
///
/// The threads rendezvous before **every** `publish` they have in common, not just
/// once before the loop, because the conflict is between two commits: attempts
/// that start apart may not overlap at all. The group that runs longer then
/// continues alone, which is deliberate — it leaves the two guards at *different*
/// frontiers, so the re-attachment at the end can tell "read group A's guard" from
/// "read the maximum of the two".
#[test]
fn two_groups_commit_concurrently_into_one_catalog() {
    let Some((dsn, dir)) = postgres_or_skip("two_groups_commit_concurrently_into_one_catalog")
    else {
        return;
    };
    let data = dir.path().join("data").display().to_string();
    let suffix = std::process::id();
    let (group_a, table_a) = (format!("ga{suffix}"), format!("ta{suffix}"));
    let (group_b, table_b) = (format!("gb{suffix}"), format!("tb{suffix}"));
    let cfg_a = postgres_group_config(&dsn, &data, &group_a, &[&table_a]);
    let cfg_b = postgres_group_config(&dsn, &data, &group_b, &[&table_b]);

    for (cfg, table) in [(&cfg_a, &table_a), (&cfg_b, &table_b)] {
        let mut catalog = Catalog::open(cfg).expect("attach the shared catalog");
        catalog
            .execute_sql(&format!(
                "CREATE TABLE dl.{table} (id BIGINT, name VARCHAR)"
            ))
            .expect("create the group's table");
    }

    // A stops after `SHARED`, B carries on to `STEPS_A + 1`, so the guards differ.
    const SHARED: u64 = 4;
    const STEPS_A: u64 = SHARED;
    const STEPS_B: u64 = STEPS_A + 2;
    const ROWS: u64 = 3;
    let (tx_a, rx_a) = std::sync::mpsc::sync_channel::<()>(1);
    let (tx_b, rx_b) = std::sync::mpsc::sync_channel::<()>(1);
    let run = |cfg: DuckLakeWriterConfig,
               table: String,
               group: String,
               steps: u64,
               send: std::sync::mpsc::SyncSender<()>,
               recv: std::sync::mpsc::Receiver<()>| {
        std::thread::spawn(move || {
            let mut catalog = Catalog::open(&cfg).expect("attach for the group");
            let (mut applied, mut retried, mut refused) = (0, 0, 0);
            for step in 0..steps {
                let batch = appended(&catalog, &group, &table, step, ROWS);
                if step < SHARED {
                    rendezvous(&send, &recv, &group);
                }
                let result = catalog
                    .publish(
                        &batch,
                        Some(std::slice::from_ref(&table)),
                        super::writer::MAX_ATTEMPTS,
                    )
                    .unwrap_or_else(|e| panic!("{group} step {step}: {e:#}"));
                match result.decision {
                    // A duplicate is a success: it means the step is committed.
                    Decision::Apply => applied += 1,
                    Decision::Duplicate => applied += 1,
                    Decision::Stale | Decision::WrongGeneration | Decision::WrongOwner => {
                        refused += 1
                    }
                }
                if result.attempts > 1 {
                    retried += 1;
                }
            }
            (catalog, applied, retried, refused)
        })
    };
    let a = run(cfg_a, table_a.clone(), group_a.clone(), STEPS_A, tx_a, rx_b);
    let b = run(cfg_b, table_b.clone(), group_b.clone(), STEPS_B, tx_b, rx_a);
    let (mut catalog_a, applied_a, retried_a, refused_a) = a.join().expect("group A's thread");
    let (mut catalog_b, applied_b, retried_b, refused_b) = b.join().expect("group B's thread");

    eprintln!("[pg] retried steps: A {retried_a} of {STEPS_A}, B {retried_b} of {STEPS_B}");
    assert_eq!(refused_a + refused_b, 0, "a refusal is a lost step");
    assert_eq!(applied_a, STEPS_A, "group A committed every step once");
    assert_eq!(applied_b, STEPS_B, "group B committed every step once");
    // Asserted, not just printed: this case exists to run the retry path, and a
    // run where the two writers never conflicted has not run it. What is *not*
    // asserted is how many conflicts there are — that is DuckLake's business.
    assert!(
        retried_a + retried_b >= 1,
        "no commit conflicted, so the retry path never ran (A {retried_a}, B {retried_b})"
    );

    for (catalog, table, steps) in [
        (&mut catalog_a, &table_a, STEPS_A),
        (&mut catalog_b, &table_b, STEPS_B),
    ] {
        // Filtered by `name` rather than counted outright: the two groups write
        // identically shaped rows, so a row that landed in the wrong table would
        // still be counted by a bare `count(*)`.
        assert_eq!(
            catalog
                .scalar(&format!(
                    "SELECT count(*) FROM dl.{table} WHERE name = '{table}'"
                ))
                .expect("count the group's own rows")
                .as_deref(),
            Some((steps * ROWS).to_string().as_str()),
            "{table} holds its own group's rows"
        );
        assert_eq!(
            catalog
                .scalar(&format!(
                    "SELECT count(*) FROM dl.{table} WHERE name <> '{table}'"
                ))
                .expect("count the foreign rows")
                .as_deref(),
            Some("0"),
            "{table} holds nobody else's rows"
        );
        assert_eq!(
            catalog.frontier().expect("the group's guard"),
            steps,
            "{table}'s guard is at its own last step"
        );
    }

    // The guard is keyed by group, so a new attachment for group A must read A's
    // frontier. The two frontiers differ, so reading `max` or B's row instead
    // would be visible.
    let other = Catalog::open(&postgres_group_config(&dsn, &data, &group_a, &[&table_a]))
        .expect("reattach the catalog");
    assert_eq!(
        other
            .frontier()
            .expect("group A's guard from a new connection"),
        STEPS_A,
        "a fresh attachment reads group A's guard, not the other group's"
    );
}

/// A resync must clear only the tables the batch carries, not every table the
/// group is configured with.
///
/// This is the §18 defect in the configuration where it can actually happen. The
/// writer assembles one endpoint's batch and commits it alone, so the endpoint's
/// *configured* set can be wider than what the batch writes; a resync scoped to
/// the configured set then deletes rows it does not repopulate, and advances the
/// guard past the hole. With one table per group — what every file-backed case
/// has — the two scopes coincide and the defect is invisible, which is why this
/// case configures the group with two tables.
#[test]
fn a_resync_clears_only_its_own_group() {
    let Some((dsn, dir)) = postgres_or_skip("a_resync_clears_only_its_own_group") else {
        return;
    };
    let data = dir.path().join("data").display().to_string();
    let suffix = std::process::id();
    let (group_a, table_a) = (format!("ra{suffix}"), format!("rat{suffix}"));
    let (group_b, keep_b, table_b) = (
        format!("rb{suffix}"),
        format!("rbk{suffix}"),
        format!("rbt{suffix}"),
    );

    for (group, tables) in [
        (&group_a, vec![table_a.as_str()]),
        (&group_b, vec![table_b.as_str(), keep_b.as_str()]),
    ] {
        let mut catalog = Catalog::open(&postgres_group_config(&dsn, &data, group, &tables))
            .expect("attach the shared catalog");
        for table in tables {
            catalog
                .execute_sql(&format!(
                    "CREATE TABLE dl.{table} (id BIGINT, name VARCHAR)"
                ))
                .expect("create the table");
        }
    }
    let mut catalog_a = Catalog::open(&postgres_group_config(&dsn, &data, &group_a, &[&table_a]))
        .expect("attach group A");
    // Group B is configured with two tables, and both hold rows before the resync.
    let mut catalog_b = Catalog::open(&postgres_group_config(
        &dsn,
        &data,
        &group_b,
        &[&table_b, &keep_b],
    ))
    .expect("attach group B");

    let append_a = appended(&catalog_a, &group_a, &table_a, 0, 2);
    assert_eq!(
        catalog_a
            .publish(
                &append_a,
                Some(std::slice::from_ref(&table_a)),
                super::writer::MAX_ATTEMPTS
            )
            .expect("group A's append")
            .decision,
        Decision::Apply
    );
    let append_b = appended(&catalog_b, &group_b, &table_b, 0, 2);
    assert_eq!(
        catalog_b
            .publish(
                &append_b,
                Some(std::slice::from_ref(&table_b)),
                super::writer::MAX_ATTEMPTS
            )
            .expect("group B's append")
            .decision,
        Decision::Apply
    );
    let seed_keep = appended(&catalog_b, &group_b, &keep_b, 1, 2);
    assert_eq!(
        catalog_b
            .publish(
                &seed_keep,
                Some(std::slice::from_ref(&keep_b)),
                super::writer::MAX_ATTEMPTS
            )
            .expect("group B's second table")
            .decision,
        Decision::Apply
    );

    // Group B resyncs `table_b` only — exactly what the writer passes. If the
    // catalog widened that to the configured set, `keep_b` would be emptied and
    // the guard would advance over the hole.
    let resync = Batch {
        resync: true,
        deltas: vec![Delta::new(
            &table_b,
            Op::Insert,
            json!({"id": 900, "name": &table_b}),
        )],
        ..appended(&catalog_b, &group_b, &table_b, 2, 0)
    };
    assert_eq!(
        catalog_b
            .publish(
                &resync,
                Some(std::slice::from_ref(&table_b)),
                super::writer::MAX_ATTEMPTS
            )
            .expect("the resync")
            .decision,
        Decision::Apply
    );

    let count = |catalog: &mut Catalog, table: &str| {
        catalog
            .scalar(&format!("SELECT count(*) FROM dl.{table}"))
            .expect("count the rows")
            .expect("a row count")
    };
    assert_eq!(
        count(&mut catalog_a, &table_a),
        "2",
        "group A keeps its rows"
    );
    assert_eq!(
        count(&mut catalog_b, &table_b),
        "1",
        "the resynced table holds the resync's single row"
    );
    assert_eq!(
        count(&mut catalog_b, &keep_b),
        "2",
        "the table the resync did not carry keeps its rows"
    );
    assert_eq!(
        catalog_a.frontier().expect("group A's guard"),
        1,
        "group A's guard is where its own step left it"
    );
    assert_eq!(
        catalog_b.frontier().expect("group B's guard"),
        3,
        "group B's guard reached its resync"
    );
}
