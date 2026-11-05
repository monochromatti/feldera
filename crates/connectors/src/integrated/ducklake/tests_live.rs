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
    catalog.publish(&first, 8).expect("the first publication");
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
        .publish(&bad, 1)
        .expect_err("a NOT NULL violation must fail the publication");
    assert!(
        format!("{error:#}").contains("NOT NULL"),
        "unexpected error: {error:#}"
    );

    assert_eq!(
        catalog.frontier(),
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
    catalog.publish(&first, 8).expect("the first publication");

    // Same identity — generation and interval — with different content.
    let mut divergent = batch(&catalog);
    divergent.deltas = vec![Delta::new(
        "orders",
        Op::Insert,
        json!({"id": 1, "name": "zzz"}),
    )];
    let error = catalog
        .publish(&divergent, 1)
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
    assert_eq!(catalog.frontier(), 1);
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
    catalog.publish(&first, 8).expect("the first publication");

    // Row 1 disappears, row 2 is rewritten: the batch is the whole content.
    let resync = Batch {
        base: 0,
        end: 5,
        deltas: vec![Delta::new(
            "orders",
            Op::Insert,
            json!({"id": 2, "name": "b2"}),
        )],
        ..batch(&catalog)
    };
    let result = catalog.publish(&resync, 8).expect("a resync applies");
    assert_eq!(result.decision, Decision::Apply);
    assert_eq!(
        catalog
            .rows("SELECT id, name FROM dl.orders ORDER BY id")
            .unwrap(),
        ["2|b2"],
        "a resync must leave exactly its own content"
    );

    let replay = catalog.publish(&resync, 8).expect("a replayed resync");
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
    catalog.publish(&first, 8).expect("the first publication");

    let update = step(
        &catalog,
        1,
        2,
        vec![
            Delta::new("orders", Op::Delete, json!({"id": 1, "name": "a"})),
            Delta::new("orders", Op::Insert, json!({"id": 1, "name": "a2"})),
        ],
    );
    catalog.publish(&update, 8).expect("the update");
    assert_eq!(
        catalog
            .rows("SELECT id, name FROM dl.orders ORDER BY id")
            .unwrap(),
        ["1|a2"],
        "a keyed update must leave exactly the post-image"
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
    catalog.publish(&first, 8).expect("two copies of one row");
    assert_eq!(
        catalog.rows("SELECT id, name FROM dl.orders").unwrap(),
        ["7|a", "7|a"]
    );

    let second = step(&catalog, 1, 2, vec![Delta::new("orders", Op::Delete, row)]);
    catalog.publish(&second, 8).expect("a single delete");
    assert_eq!(
        catalog.rows("SELECT id, name FROM dl.orders").unwrap(),
        ["7|a"],
        "the delete removed every copy instead of one"
    );
}

/// `extension_directory` is applied with DuckDB's current setting, not the
/// deprecated one.
///
/// DuckDB 2.0 deprecated `extension_directory` in favour of
/// `extension_directories`, which is a list and refuses a bare path: writing the
/// bare path returns a config error that `duckdb_open_ext` ignores, after which
/// extensions come from the default directory or the network.  The failure this
/// test prevents is exactly that silent misconfiguration — a deployment that
/// vendored an extension and still does not find it.
#[test]
fn the_extension_directory_is_applied_as_the_current_setting() {
    let Some(config) = live_config("extension-directories") else {
        return;
    };
    let expected = config
        .catalog
        .extension_directory
        .clone()
        .expect("the test config sets one");

    let mut catalog = Catalog::open(&config).expect("attach a fresh catalog");
    let applied = catalog
        .scalar("SELECT current_setting('extension_directories')")
        .expect("read the setting")
        .expect("a value");
    assert!(
        applied.contains(&expected),
        "the directory {expected:?} was not applied through extension_directories; the \
         deprecated singular setting was used instead ({applied})"
    );
    // And the extension it points at actually loaded, so the setting was not just
    // recorded in a table.
    assert_eq!(
        catalog
            .scalar("SELECT count(*) FROM duckdb_extensions() WHERE extension_name = 'ducklake' AND loaded")
            .expect("read the loaded extensions"),
        Some("1".to_owned())
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
