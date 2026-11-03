//! The DuckLake reader: the table once, then its change feed.
//!
//! A DuckLake catalog does not offer a subscription.  What it offers is a total
//! order of committed snapshots plus a function that answers "what changed in
//! this table between snapshot A and snapshot B".  Every publication by the
//! writer (see `catalog.rs`) is exactly one snapshot, so the reader is a cursor
//! over that order:
//!
//! 1. **Bootstrap.** Read the whole table and remember the snapshot it was read
//!    at.  Both happen inside *one* DuckDB transaction, because a read that took
//!    the snapshot number first and the rows second would race a commit into
//!    either a double count (the rows are in both the snapshot and the feed) or a
//!    missed row (in neither), and neither is recoverable downstream.
//! 2. **Follow.** For every newly committed snapshot, ask
//!    `ducklake_table_changes(alias, schema, table, applied + 1, current)`.  The
//!    window is inclusive at both ends — reading `(2, 3)` returns snapshot 2's
//!    inserts as well as snapshot 3's — which is why the lower bound is
//!    `applied + 1` and not `applied`.
//!
//! # Which snapshot function
//!
//! `ducklake_current_snapshot(catalog)` reports the **last committed** snapshot:
//! it returns 3 both in a fresh session and inside a transaction that has
//! uncommitted writes (which would become 4), and it is not `NULL` in a
//! read-only session.  `ducklake_last_committed_snapshot(catalog)` sounds like
//! the right one and is not: in a session that has not itself committed anything
//! it returns `NULL`.  Both facts were measured against a real catalog, and the
//! reader's bootstrap depends on the first.
//!
//! # Weights
//!
//! The feed labels each row with a `change_type`, and that label is the weight:
//!
//! | `change_type`      | weight |
//! |--------------------|--------|
//! | `insert`           | +1     |
//! | `delete`           | -1     |
//! | `update_preimage`  | -1     |
//! | `update_postimage` | +1     |
//!
//! This is why the writer emits an update as a preimage/postimage pair rather
//! than a delete followed by an insert: the pair is one snapshot, so a reader
//! never observes the instant when the row does not exist.
//!
//! An unrecognized `change_type` is an error, not a skipped row: DuckLake gaining
//! a fifth kind of change must fail loudly rather than silently lose data.
//!
//! # JSON with explicit polarity, not Arrow
//!
//! The endpoint is an [`IntegratedInputEndpoint`], so it hands records to the
//! collection itself.  [`DeCollectionStream`] accepts one record plus a polarity
//! (`insert` or `delete`), which is what a change feed *is*.  The Arrow
//! alternative (`ArrowStream::insert_with_polarities`) would batch better, but it
//! needs DuckDB's Arrow C stream turned into `RecordBatch`es — a lot of FFI for
//! something no measurement has asked for.  The rows are already JSON
//! (`to_json`, projected onto the table's columns), so this is also the shortest
//! path from catalog to circuit.
//!
//! # At-least-once, not exactly-once
//!
//! A checkpoint records the snapshot whose rows were flushed, so a restart
//! resumes at `snapshot + 1` and does not replay the flushed window — but the
//! flush and the engine's checkpoint are not one atomic act, so a failure in
//! between replays a window.  Promising exactly once would require the read to be
//! transactional with the checkpoint, which is the same reason the writer keeps
//! its own guard row.
//!
//! # One process per file catalog
//!
//! DuckLake's metadata lives in a DuckDB file (or a PostgreSQL database).  A file
//! admits a single process, so a reader and a writer in different processes
//! cannot share one: the second `ATTACH` fails with an I/O error.  That is a
//! property of the catalog, not of this connector — PostgreSQL is how one runs a
//! pipeline and a writer side by side.

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::Duration,
};

use anyhow::{Context, Result as AnyResult, bail};
use feldera_adapterlib::{
    catalog::{DeCollectionStream, InputCollectionHandle, RecordFormat},
    format::BufferSize,
    transport::{InputConsumer, InputReader, InputReaderCommand, Resume},
};
use feldera_types::{
    program_schema::Relation,
    transport::ducklake::{DuckLakeCatalogConfig, DuckLakeReaderConfig},
};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};

use super::{
    catalog::{attach_statement, quote_ident, sql_literal},
    ffi::{Conn, Db, Library, Rows},
};

/// How often to look for the controller's `Queue` while rows are buffered.  A
/// step cannot start until the buffered rows are flushed, so this is short.
const FLUSH_INTERVAL: Duration = Duration::from_millis(2);

/// Where the reader stopped, as stored in a checkpoint.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, Serialize)]
pub(super) struct ReaderState {
    /// The snapshot number whose rows have all been handed to the collection.
    pub(super) snapshot: u64,
    /// Whether the bootstrap read has happened.  A checkpoint from a reader that
    /// never got that far must bootstrap, not resume at snapshot 0 — those are
    /// different reads, and the second one misses the table that already exists.
    pub(super) bootstrapped: bool,
}

impl ReaderState {
    fn from_metadata(metadata: Option<&serde_json::Value>) -> Self {
        metadata
            .and_then(|value| serde_json::from_value(value.clone()).ok())
            .unwrap_or_default()
    }

    fn to_metadata(self) -> serde_json::Value {
        serde_json::to_value(self).expect("a reader state is plain JSON")
    }
}

/// The reader handed to the controller: a channel to its worker thread.
pub struct DuckLakeInputReader {
    sender: UnboundedSender<InputReaderCommand>,
    /// Set by the worker as it returns, so that dropping the reader can wait for
    /// the DuckDB work to stop rather than exit with a query in flight.
    finished: Arc<AtomicBool>,
}

impl Drop for DuckLakeInputReader {
    fn drop(&mut self) {
        // The worker may be in the middle of a DuckDB call.  Letting the process
        // tear down the library underneath it segfaults, so ask it to stop and
        // wait — bounded, because a wedged worker must not hang shutdown.
        let _ = self.sender.send(InputReaderCommand::Disconnect);
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while !self.finished.load(Ordering::Acquire) {
            if std::time::Instant::now() > deadline {
                return;
            }
            thread::sleep(Duration::from_millis(5));
        }
    }
}

impl InputReader for DuckLakeInputReader {
    fn as_any(self: Arc<Self>) -> Arc<dyn std::any::Any + Send + Sync> {
        self
    }

    fn request(&self, command: InputReaderCommand) {
        let _ = self.sender.send(command);
    }

    fn is_closed(&self) -> bool {
        self.sender.is_closed()
    }
}

/// Start the reader for one table.
pub fn open(
    endpoint_name: &str,
    config: DuckLakeReaderConfig,
    relation: &Relation,
    handle: &InputCollectionHandle,
    consumer: Box<dyn InputConsumer>,
    resume: Option<serde_json::Value>,
) -> AnyResult<Box<dyn InputReader>> {
    let stream = handle
        .handle
        .configure_deserializer(RecordFormat::Json(Default::default()))
        .map_err(|error| {
            anyhow::anyhow!(
                "the DuckLake input {endpoint_name:?} cannot feed {}.{} as JSON: {error}",
                config.catalog.schema,
                config.table
            )
        })?;
    let table = Table {
        projection: projection(relation)?,
        alias: quote_ident(&config.catalog.alias)?,
        catalog: config.catalog.clone(),
        schema: config.catalog.schema.clone(),
        table: config.table.clone(),
    };
    let state = ReaderState::from_metadata(resume.as_ref());
    // The interval is the pipeline's decision, so it is read here rather than
    // fixed: a test and a low-latency pipeline want very different values.
    let poll_interval: Duration = config.poll_interval().into();

    let (sender, receiver) = unbounded_channel();
    let finished = Arc::new(AtomicBool::new(false));
    let worker_finished = Arc::clone(&finished);
    thread::Builder::new()
        .name(format!("ducklake-input-{endpoint_name}"))
        .spawn(move || {
            let result = run(
                table,
                stream,
                consumer.as_ref(),
                receiver,
                state,
                poll_interval,
            );
            worker_finished.store(true, Ordering::Release);
            if let Err(error) = result {
                consumer.error(true, error, None);
            }
        })
        .context("spawning the DuckLake reader")?;

    Ok(Box::new(DuckLakeInputReader { sender, finished }))
}

/// Everything needed to address one table in a catalog.
#[derive(Debug, Clone)]
pub(super) struct Table {
    /// `{'col': "col", ...}` over the relation's columns.  The change feed
    /// interleaves its own `snapshot_id`, `rowid` and `change_type` columns with
    /// the table's, so `to_json(*)` would carry those along into a row that no
    /// longer matches the view.
    projection: String,
    catalog: DuckLakeCatalogConfig,
    schema: String,
    table: String,
    /// The catalog alias, quoted for use in a path.
    alias: String,
}

impl Table {
    /// A table address, with the JSON projection given by the caller.  The
    /// projection normally comes from the relation ([`projection`]); a test that
    /// has no relation passes the one it wants to read.
    pub(super) fn new(
        catalog: &DuckLakeCatalogConfig,
        schema: &str,
        table: &str,
        projection: &str,
    ) -> AnyResult<Self> {
        Ok(Self {
            projection: projection.to_owned(),
            catalog: catalog.clone(),
            schema: schema.to_owned(),
            table: table.to_owned(),
            alias: quote_ident(&catalog.alias)?,
        })
    }

    fn qualified_name(&self) -> String {
        format!(
            "{}.{}.{}",
            self.alias,
            quote_ident(&self.schema).unwrap_or_else(|_| self.schema.clone()),
            quote_ident(&self.table).unwrap_or_else(|_| self.table.clone())
        )
    }

    /// The whole table as JSON rows.
    fn snapshot_rows(&self, conn: &Conn<'_>) -> AnyResult<Rows> {
        let sql = format!(
            "SELECT to_json({})::VARCHAR FROM {}",
            self.projection,
            self.qualified_name()
        );
        conn.query(&sql, &[])
            .with_context(|| format!("reading the snapshot of {}.{}", self.schema, self.table))
    }

    /// The rows that changed in snapshots `after ..= up_to`, each preceded by its
    /// change type.
    fn changes(&self, conn: &Conn<'_>, after: u64, up_to: u64) -> AnyResult<Rows> {
        let sql = format!(
            "SELECT change_type, to_json({})::VARCHAR FROM ducklake_table_changes({}, {}, {}, {after}, {up_to})",
            self.projection,
            sql_literal(&self.catalog.alias)?,
            sql_literal(&self.schema)?,
            sql_literal(&self.table)?,
        );
        conn.query(&sql, &[]).with_context(|| {
            format!(
                "reading the changes to {}.{} in snapshots ({after}, {up_to}]",
                self.schema, self.table
            )
        })
    }
}

/// Hand one row on with the polarity its change type implies.
pub(super) fn push(
    sink: &mut dyn FnMut(&str, &str) -> AnyResult<()>,
    table: &Table,
    change_type: &str,
    json: &str,
) -> AnyResult<()> {
    match change_type {
        "insert" | "update_postimage" | "delete" | "update_preimage" => sink(change_type, json),
        other => bail!(
            "{}.{} changed with change_type {other:?}, which this connector does not know the \
             weight of; refusing to guess",
            table.schema,
            table.table
        ),
    }
}

/// Build the JSON projection for a relation.
fn projection(relation: &Relation) -> AnyResult<String> {
    if relation.fields.is_empty() {
        bail!("the DuckLake input table has no columns");
    }
    let mut fields = Vec::with_capacity(relation.fields.len());
    for field in &relation.fields {
        let name = field.name.name();
        fields.push(format!("{}: {}", sql_literal(&name)?, quote_ident(&name)?));
    }
    Ok(format!("{{{}}}", fields.join(", ")))
}

/// A read-only attachment to the catalog.
pub(super) struct ReaderCatalog<'a> {
    #[allow(dead_code)] // held so that `conn` outlives its database
    db: Db<'a>,
    conn: Conn<'a>,
    catalog: DuckLakeCatalogConfig,
}

impl<'a> ReaderCatalog<'a> {
    pub(super) fn attach(catalog: &DuckLakeCatalogConfig) -> AnyResult<Self> {
        let library = Library::load()?;
        let config = library.config()?;
        if let Some(dir) = &catalog.extension_directory {
            library.set_config(config, "extension_directory", dir)?;
        }
        let db = Db::open(library, ":memory:", config)
            .with_context(|| format!("opening DuckDB for catalog {:?}", catalog.catalog))?;
        let conn = db.connect()?;

        // `LOAD` first and `INSTALL` only if it is missing — the same order, for
        // the same reason, as the writer: a vendored extension needs no egress.
        if conn.query("LOAD ducklake", &[]).is_err() {
            conn.execute("INSTALL ducklake", &[])
                .context("installing the ducklake extension")?;
            conn.execute("LOAD ducklake", &[])?;
        }
        conn.execute(&attach_statement(catalog, true)?, &[])
            .with_context(|| format!("attaching catalog {:?} read-only", catalog.catalog))?;
        Ok(Self {
            db,
            conn,
            catalog: catalog.clone(),
        })
    }

    /// Re-attach the catalog, so that commits made since the last poll become
    /// visible.
    ///
    /// DuckLake reads the catalog's metadata when the catalog is *attached* and
    /// keeps answering from that copy: an attachment that stays open keeps
    /// reporting the snapshot it saw at attach time, so a reader that never
    /// re-attached would follow nothing at all after its bootstrap gap (measured
    /// against a file catalog: a writer committing in another DuckDB instance
    /// left a long-lived attachment reporting the old snapshot).  There is no
    /// notification primitive to wait on instead, and re-attaching is cheap — a
    /// metadata read of a small DuckDB file.
    ///
    /// This must run outside a transaction: DuckDB refuses `ATTACH` inside one.
    fn refresh(&self) -> AnyResult<()> {
        let alias = quote_ident(&self.catalog.alias)?;
        let _ = self.conn.execute(&format!("DETACH {alias}"), &[]);
        self.conn
            .execute(&attach_statement(&self.catalog, true)?, &[])
            .with_context(|| format!("re-attaching catalog {:?}", self.catalog.catalog))
    }

    /// `ducklake_current_snapshot` — the last *committed* snapshot, which is also
    /// the snapshot this transaction reads (see the module docs for why not
    /// `ducklake_last_committed_snapshot`).
    fn current_snapshot(&self) -> AnyResult<u64> {
        let sql = format!(
            "SELECT * FROM ducklake_current_snapshot({})",
            sql_literal(&self.catalog.alias)?
        );
        let rows = self.conn.query(&sql, &[])?.into_rows();
        let Some(Some(value)) = rows.first().and_then(|row| row.first()) else {
            bail!(
                "ducklake_current_snapshot({:?}) returned no snapshot",
                self.catalog.alias
            );
        };
        value
            .parse()
            .with_context(|| format!("parsing the snapshot number {value:?}"))
    }

    /// Run `body` in a transaction, committing only if it succeeds.  The read is
    /// transactional so that the snapshot number and the rows read under it
    /// agree.
    fn transaction<T>(&self, body: impl FnOnce(&Self) -> AnyResult<T>) -> AnyResult<T> {
        self.conn.execute("BEGIN TRANSACTION", &[])?;
        match body(self) {
            Ok(value) => {
                self.conn.execute("COMMIT", &[])?;
                Ok(value)
            }
            Err(error) => {
                let _ = self.conn.execute("ROLLBACK", &[]);
                Err(error)
            }
        }
    }
}

/// One read attempt: bootstrap, or replay one window of changes.
pub(super) fn advance(
    catalog: &ReaderCatalog<'_>,
    table: &Table,
    sink: &mut dyn FnMut(&str, &str) -> AnyResult<()>,
    state: ReaderState,
) -> AnyResult<ReaderState> {
    catalog.refresh()?;
    catalog.transaction(|catalog| {
        let current = catalog.current_snapshot()?;
        if !state.bootstrapped {
            for row in table.snapshot_rows(&catalog.conn)?.into_rows() {
                push(sink, table, "insert", json_of(&row)?)?;
            }
            return Ok(ReaderState {
                snapshot: current,
                bootstrapped: true,
            });
        }
        if current <= state.snapshot {
            return Ok(state);
        }
        for row in table
            .changes(&catalog.conn, state.snapshot + 1, current)?
            .into_rows()
        {
            let [Some(change_type), Some(json)] = row.as_slice() else {
                bail!("a change row is missing its change type or payload: {row:?}");
            };
            push(sink, table, change_type, json)?;
        }
        Ok(ReaderState {
            snapshot: current,
            ..state
        })
    })
}

/// The one column of a row projected by [`Table::snapshot_rows`].
fn json_of(row: &[Option<String>]) -> AnyResult<&str> {
    match row {
        [Some(json)] => Ok(json),
        _ => bail!("expected one JSON column, got {row:?}"),
    }
}

/// The worker loop: answer the controller, and poll the catalog while it is not
/// paused.
fn run(
    table: Table,
    mut stream: Box<dyn DeCollectionStream>,
    consumer: &dyn InputConsumer,
    mut receiver: UnboundedReceiver<InputReaderCommand>,
    mut state: ReaderState,
    poll_interval: Duration,
) -> AnyResult<()> {
    let mut catalog = ReaderCatalog::attach(&table.catalog)?;
    let mut extending = true;
    loop {
        loop {
            match receiver.try_recv() {
                Ok(InputReaderCommand::Extend) => extending = true,
                Ok(InputReaderCommand::Pause) => extending = false,
                Ok(InputReaderCommand::Queue { .. }) => {
                    // Always answer the `Queue`, even with nothing buffered: the
                    // controller's step waits for this signal, so staying silent
                    // when the catalog had no news deadlocks the pipeline.  A
                    // reader that has not bootstrapped yet has no resume point to
                    // offer, which is what `Resume::Barrier` says.
                    let amt = stream.len();
                    if !amt.is_empty() {
                        stream.flush()?;
                    }
                    let resume = if state.bootstrapped {
                        Resume::new_metadata_only(state.to_metadata(), None)
                    } else {
                        Resume::Barrier
                    };
                    consumer.extended(amt, Some(resume), vec![]);
                }
                Ok(InputReaderCommand::Replay { metadata, .. }) => {
                    // The controller replays from a checkpoint: drop whatever is
                    // buffered, then re-read from the recorded snapshot.
                    stream.truncate(0);
                    state = ReaderState::from_metadata(Some(&metadata));
                    if !state.bootstrapped {
                        // A replay of the initial state re-runs the bootstrap.
                        state = ReaderState::default();
                    }
                    catalog = ReaderCatalog::attach(&table.catalog)?;
                    consumer.replayed(BufferSize::empty(), 0);
                }
                Ok(InputReaderCommand::Disconnect) => return Ok(()),
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => return Ok(()),
            }
        }

        if extending && stream.len().is_empty() {
            let mut sink = |change_type: &str, json: &str| match change_type {
                "insert" | "update_postimage" => stream.insert(json.as_bytes(), &None),
                _ => stream.delete(json.as_bytes(), &None),
            };
            let next = advance(&catalog, &table, &mut sink, state)?;
            if next != state {
                state = next;
            }
        }
        thread::sleep(if stream.len().is_empty() {
            poll_interval
        } else {
            FLUSH_INTERVAL
        });
    }
}
