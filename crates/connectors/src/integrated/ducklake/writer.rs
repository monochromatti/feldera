//! The DuckLake output connector: one DuckLake transaction per step.
//!
//! The writer is simultaneously the [`OutputEndpoint`], the [`Encoder`] and the
//! [`OutputConsumer`], the way the Delta Lake writer is, because a step's rows
//! and a step's identity arrive on two different hooks: the controller calls
//! `consumer().batch_start(transaction, kind)`, then `encode(batch)`, then
//! `consumer().batch_end()`.  Only something that sees all three can apply a
//! step's rows and its guard update in one transaction, which is the whole
//! point of the connector.
//!
//! # Why a failed publication blocks instead of returning
//!
//! Returning an error from `encode`/`batch_end` is not the same as refusing the
//! step.  The engine reports the fault and counts what it has already handed
//! over; if the publish failed *after* the catalog committed, a retry would be
//! a duplicate (harmless, the guard catches it) — but if it failed *before*, a
//! step that the engine considers processed is output that no retry will
//! deliver.  So a failed publication reports itself through the endpoint's
//! callbacks and then holds the output thread until shutdown, which is what the
//! Python bridge does and what the design notes require.  Holding is safe here
//! because `batch_end` runs on the engine's output thread, which is only ever
//! joined *after* the shutdown token is cancelled — so the wait is bounded by
//! the shutdown the token announces, and there is no path that waits on it
//! while the pipeline is running.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use anyhow::{Result as AnyResult, anyhow, bail};
use feldera_adapterlib::catalog::{RecordFormat, SerBatchReader};
use feldera_adapterlib::transport::{OutputBatchType, OutputEndpointCallbacks};
use feldera_types::format::json::JsonFlavor;
use feldera_types::program_schema::Relation;
use feldera_types::transport::ducklake::DuckLakeWriterConfig;
use tokio_util::sync::CancellationToken;

use super::catalog::Catalog;
use super::protocol::{self, Batch, Delta, Op};
use crate::controller::{ControllerInner, EndpointId};
use crate::transport::Step;
use crate::{ControllerError, Encoder, OutputConsumer, OutputEndpoint};

/// DuckLake output connector.
///
/// Implements [`OutputConsumer`], [`Encoder`] and [`OutputEndpoint`] on one
/// type: a step's rows and a step's identity arrive through different hooks, and
/// only something that sees both can apply them in one transaction.
pub struct DuckLakeWriter {
    #[allow(dead_code)] // kept for the metrics/health wiring the controller expects
    endpoint_id: EndpointId,
    endpoint_name: String,
    config: DuckLakeWriterConfig,

    /// Columns the engine writes, i.e. the schema the DuckLake table must have.
    /// A table with a different one is a configuration error the writer reports,
    /// not silently adapts to.
    #[allow(dead_code)]
    value_schema: Relation,

    /// Key columns when the view is an index, else `None`.
    #[allow(dead_code)]
    key_schema: Option<Relation>,

    /// The controller, for reporting progress and failures.  Weak because the
    /// controller owns the endpoint.
    #[allow(dead_code)]
    controller: Weak<ControllerInner>,

    /// Rows written in the batch in flight, for the endpoint's status line.
    records_written: Arc<AtomicU64>,

    /// Cancelled when the pipeline shuts down.  The failure path waits on it.
    shutdown: CancellationToken,

    /// The attached catalog, present from `connect` onwards.
    catalog: Option<Catalog>,

    /// The table this endpoint writes and its key columns.
    target: Option<Target>,

    /// Callbacks back to the controller, set by `connect`.
    callbacks: Option<Arc<dyn OutputEndpointCallbacks>>,

    /// The batch in flight: what `batch_start` declared and `encode` filled.
    pending: Pending,
}

/// The DuckLake table one endpoint writes.
#[derive(Clone, Debug)]
struct Target {
    table: String,
}

/// The batch between `batch_start` and `batch_end`.
#[derive(Default)]
struct Pending {
    step: Option<Step>,
    resync: bool,
    deltas: Vec<Delta>,
}

impl DuckLakeWriter {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        endpoint_id: EndpointId,
        endpoint_name: &str,
        config: &DuckLakeWriterConfig,
        key_schema: &Option<Relation>,
        value_schema: &Relation,
        controller: Weak<ControllerInner>,
        shutdown: CancellationToken,
        _continue_previous_state: bool,
        is_index: bool,
        enable_output_buffer: bool,
    ) -> Result<Self, ControllerError> {
        // An indexed z-set carries a key the connector would have to honour for
        // `COUNT`-style views to stay consistent; this sink writes plain rows, so
        // accepting the flag and ignoring it would silently change the semantics of
        // a view that asked to be indexed.  Postgres and DynamoDB refuse the
        // combination for the same reason.
        if is_index {
            return Err(ControllerError::not_supported(
                "The DuckLake output connector does not support indexed views. Remove the `index` \
                 property from the view, or use a connector that supports it.",
            ));
        }
        // Several tables may be configured for one endpoint, and each endpoint
        // publishes only the table its view names (`resolve_target`).  That is
        // safe because the guard is per `output_group`; what is *not* safe is two
        // endpoints sharing one group, and the engine gives each endpoint its own
        // transaction, so neither of them can publish the group's content in one
        // commit.  The reference implementation can, because it assembles a group
        // before publishing; this endpoint cannot, so one group must carry exactly
        // one publishing endpoint.  That is a deployment rule the connector cannot
        // check from inside a single endpoint's configuration.
        let _ = &config.tables;
        // A buffered step is a different step: the controller would hand this
        // writer a batch covering several steps, and the guard row can name only
        // one.  Refusing at construction is better than publishing output the
        // catalog cannot account for.
        if enable_output_buffer {
            return Err(ControllerError::NotSupported {
                error: "enable_output_buffer on a DuckLake output: the connector publishes one \
                        step per transaction, so a batch must not span steps"
                    .to_string(),
            });
        }
        Ok(Self {
            endpoint_id,
            endpoint_name: endpoint_name.to_string(),
            config: config.clone(),
            value_schema: value_schema.clone(),
            key_schema: key_schema.clone(),
            controller,
            records_written: Arc::new(AtomicU64::new(0)),
            shutdown,
            catalog: None,
            target: None,
            callbacks: None,
            pending: Pending::default(),
        })
    }

    /// Resolve the single table this endpoint writes.
    ///
    /// The configuration maps *views* to tables because a pipeline may publish
    /// several; an endpoint is created per view, so exactly one entry applies.
    /// More than one is ambiguous and is refused rather than guessed at.
    fn resolve_target(&self) -> AnyResult<Target> {
        let tables = &self.config.tables;
        if tables.is_empty() {
            bail!(
                "the DuckLake output connector (endpoint {}) has no output tables configured",
                self.endpoint_name
            );
        }
        // The view name is matched exactly, and only exactly.  This used to accept
        // `view.replace('.', "_") == endpoint_name` as well, which makes views `a.b`
        // and `a_b` both match an endpoint called `a_b` — two entries claiming one
        // endpoint, resolved by whichever came first in a `BTreeMap`.  A name that
        // does not match is refused with the names that were configured.
        let chosen: Vec<(&String, _)> = tables
            .iter()
            .filter(|(view, _)| **view == self.endpoint_name)
            .collect();
        let chosen = if chosen.len() == 1 {
            chosen
        } else if tables.len() == 1 {
            tables.iter().collect()
        } else {
            bail!(
                "the DuckLake output connector (endpoint {}) cannot tell which of {:?} it writes; \
                 name the view in `tables` after the endpoint",
                self.endpoint_name,
                tables.keys().collect::<Vec<_>>()
            );
        };
        let (_, sink) = chosen[0];
        Ok(Target {
            table: sink.table.clone(),
        })
    }

    /// Attach the catalog and resolve the output table, once.
    ///
    /// An integrated endpoint never gets `connect` from the controller: the
    /// controller creates it with [`create_integrated_output_endpoint`] and then
    /// takes [`IntegratedOutputEndpoint::into_encoder`], so the object that runs
    /// the steps has had no connection attempt.  The setup therefore happens on
    /// first use.  `connect` does the same work for a caller that does call it;
    /// both are idempotent.
    fn ensure_connected(&mut self) -> AnyResult<()> {
        if self.catalog.is_some() {
            return Ok(());
        }
        self.target = Some(self.resolve_target()?);
        let catalog = Catalog::open(&self.config)?;
        tracing::info!(
            "ducklake: {} attached {:?} at frontier {} epoch {}",
            self.endpoint_name,
            self.config.catalog.catalog,
            catalog.frontier().unwrap_or_default(),
            catalog.epoch()
        );
        self.catalog = Some(catalog);
        Ok(())
    }

    /// Publish the batch in flight, or report and hold.
    fn publish_pending(&mut self) {
        let Some(step) = self.pending.step else {
            return;
        };
        let deltas = std::mem::take(&mut self.pending.deltas);
        let resync = std::mem::take(&mut self.pending.resync);
        if !resync && deltas.is_empty() {
            // An empty step is published by advancing nothing: the engine skips
            // empty transactions before reaching the encoder, so this only
            // happens when a step's rows cancelled out exactly.
            return;
        }
        let rows = deltas.len() as u64;
        match self.publish(step, deltas, resync) {
            // Report the frontier of the guard row this publication wrote, not a
            // fresh read: a read that fails must not turn into "frontier 0".
            Ok(frontier) => self.report_progress(frontier, rows),
            Err(e) => self.fail_and_hold(e),
        }
    }

    /// Publish one step and return the frontier the guard row now carries.
    fn publish(&mut self, step: Step, deltas: Vec<Delta>, resync: bool) -> AnyResult<u64> {
        self.ensure_connected()?;
        let catalog = self
            .catalog
            .as_mut()
            .ok_or_else(|| anyhow!("the DuckLake writer was never connected"))?;
        let target = self
            .target
            .as_ref()
            .ok_or_else(|| anyhow!("the DuckLake writer has no output table"))?;

        // The base is the frontier the batch was assembled against.  Taking it
        // from the guard rather than from the previous step means a step the
        // engine skipped (one with no output) is not mistaken for a gap.
        // A guard row that cannot be read must stop the publication: defaulting to
        // frontier 0 would make every batch look like a duplicate of the first one.
        //
        // A resync is based on nothing rather than on the frontier: it is the whole
        // content as of `step`, so it must apply from any frontier, which is what
        // makes it the recovery path after a restart.  The reference implementation
        // publishes it the same way.
        let base = if resync { 0 } else { catalog.frontier()? };
        let batch = Batch {
            output_group: self.config.output_group.clone(),
            generation: self.config.generation.clone(),
            epoch: catalog.epoch(),
            base,
            end: step,
            deltas,
            resync,
        };
        protocol::validate(&batch, std::slice::from_ref(&target.table))?;
        // The payload is this endpoint's table only, and a resync deletes only what
        // it then repopulates.  Clearing every configured table here would let a
        // second endpoint's resync wipe the rows a first one just published, with
        // both guards advancing past them.
        let result = catalog.publish(
            &batch,
            Some(std::slice::from_ref(&target.table)),
            MAX_ATTEMPTS,
        )?;
        match result.decision {
            protocol::Decision::Apply => {
                tracing::debug!(
                    "ducklake: {} published {} ({} deltas, {} attempt(s))",
                    self.endpoint_name,
                    batch.batch_id(),
                    batch.deltas.len(),
                    result.attempts
                );
                Ok(result.guard.frontier)
            }
            protocol::Decision::Duplicate => {
                tracing::info!(
                    "ducklake: {} skipped {} - already committed",
                    self.endpoint_name,
                    batch.batch_id()
                );
                Ok(result.guard.frontier)
            }
            other => {
                // A frontier ahead of this batch is the one situation an operator
                // can recover from without a code change, so say how.
                let hint = if other == protocol::Decision::Stale {
                    ", the catalog is ahead of this pipeline; restart it with \
                     send_snapshot enabled to resynchronise"
                } else {
                    ""
                };
                bail!(
                    "ducklake: {} refused {}: {} (frontier {}){hint}",
                    self.endpoint_name,
                    batch.batch_id(),
                    other.as_str(),
                    result.guard.frontier
                )
            }
        }
    }

    /// Report a failure and hold the output thread until shutdown.
    ///
    /// See the module notes: returning here would let the engine treat a step
    /// whose output is not in the catalog as processed.
    fn fail_and_hold(&mut self, error: anyhow::Error) {
        let detail = format!("{error:#}");
        tracing::error!(
            "ducklake: {} failed to publish: {detail}",
            self.endpoint_name
        );
        if let Some(callbacks) = &self.callbacks {
            callbacks.error(
                true,
                anyhow!("the DuckLake connector could not publish a step: {detail}"),
                Some("ducklake_publication"),
            );
        }
        // Hold until the pipeline tears down.  The sleep bounds the latency of
        // noticing the cancellation, and the token is the only thing that can
        // end this loop.
        while !self.shutdown.is_cancelled() {
            std::thread::sleep(HOLD_POLL);
        }
    }

    fn report_progress(&self, frontier: u64, rows: u64) {
        // `records_written` is a record count in the engine's metrics; feeding it a
        // frontier made progress reporting lie.  The frontier is traced instead.
        tracing::debug!("ducklake: {} is at frontier {frontier}", self.endpoint_name);
        self.records_written.store(rows, Ordering::Relaxed);
    }

    /// Turn the batch's rows into deltas.
    ///
    /// Feldera hands the encoder a batch reader, so the rows are read directly in
    /// their JSON representation rather than through a serialized transport
    /// buffer: the writer and the format are the same object here.
    fn collect_deltas(&mut self, batch: &Arc<dyn SerBatchReader>) -> AnyResult<Vec<Delta>> {
        let table = self
            .target
            .as_ref()
            .ok_or_else(|| anyhow!("the DuckLake writer has no output table"))?
            .table
            .clone();
        let mut cursor = batch.cursor(RecordFormat::Json(JsonFlavor::Default))?;
        let mut deltas = Vec::new();
        let mut buffer = Vec::new();
        while cursor.key_valid() {
            if !cursor.val_valid() {
                cursor.step_key()?;
                continue;
            }
            let mut weight = cursor.weight();
            if weight.unsigned_abs() > MAX_MULTIPLICITY as u64 {
                bail!(
                    "a row on {table:?} has weight {weight}; the DuckLake connector applies rows \
                     one at a time and will not expand a multiplicity that large (adjust the query \
                     or add DISTINCT)"
                );
            }
            buffer.clear();
            cursor.serialize_key(&mut buffer)?;
            let row: serde_json::Value = serde_json::from_slice(&buffer)
                .map_err(|e| anyhow!("row for {table:?} is not valid JSON: {e}"))?;
            let op = if weight > 0 { Op::Insert } else { Op::Delete };
            while weight != 0 {
                deltas.push(Delta::new(table.clone(), op, row.clone()));
                weight -= if weight > 0 { 1 } else { -1 };
            }
            cursor.step_key()?;
        }
        Ok(deltas)
    }
}

/// How many times a losing publication is retried before it is reported.
///
/// A conflict means another publisher committed between our read and our
/// commit; that is a race the retry resolves, not an error, but a persistent
/// conflict is a real problem and must not spin forever.
const MAX_ATTEMPTS: usize = 8;

/// How often the failure path checks for shutdown while it holds the thread.
const HOLD_POLL: Duration = Duration::from_millis(200);

/// Largest row multiplicity expanded in place.
const MAX_MULTIPLICITY: i64 = 1024;

impl OutputConsumer for DuckLakeWriter {
    fn max_buffer_size_bytes(&self) -> usize {
        // Rows are read from the engine's own batch representation, so there is
        // no transport buffer to bound.
        usize::MAX
    }

    fn batch_start(&mut self, step: Step, batch_type: OutputBatchType) {
        self.records_written.store(0, Ordering::Relaxed);
        let resync = match batch_type {
            OutputBatchType::Snapshot => {
                // The engine is starting over: it sends the whole content rather
                // than a delta.  `send_snapshot: false` documents that a deployment
                // which suspects the catalog and the engine disagree would rather
                // fail than have its tables rewritten, so this is where that promise
                // is kept — until now the flag was accepted and never read.
                if !self.config.send_snapshot {
                    self.fail_and_hold(anyhow!(
                        "the engine sent a snapshot for {} but `send_snapshot` is false; \
                         the catalog was not rewritten",
                        self.endpoint_name
                    ));
                    return;
                }
                true
            }
            OutputBatchType::Delta => false,
        };
        self.pending = Pending {
            step: Some(step),
            resync,
            deltas: Vec::new(),
        };
    }

    fn push_buffer(&mut self, _buffer: &[u8], _num_records: usize) {
        unreachable!("the DuckLake connector consumes batches, not buffers")
    }

    fn push_key(
        &mut self,
        _key: Option<&[u8]>,
        _val: Option<&[u8]>,
        _headers: &[(&str, Option<&[u8]>)],
        _num_records: usize,
    ) {
        unreachable!("the DuckLake connector consumes batches, not buffers")
    }

    fn batch_end(&mut self) {
        self.publish_pending();
    }
}

impl Encoder for DuckLakeWriter {
    fn consumer(&mut self) -> &mut dyn OutputConsumer {
        self
    }

    fn encode(&mut self, batch: Arc<dyn SerBatchReader>) -> AnyResult<()> {
        // A step whose rows cancel out arrives with content that nets to
        // nothing; publishing it would still advance the frontier, which is
        // correct, so the deltas are collected even when they are empty.
        let deltas = match self
            .ensure_connected()
            .and_then(|()| self.collect_deltas(&batch))
        {
            Ok(deltas) => deltas,
            Err(e) => {
                // Holding here is the only place this loss can be stopped.  The
                // controller reports an encoder error non-fatally and calls
                // `batch_end` afterwards, and a `batch_end` with nothing collected
                // is indistinguishable from a step whose rows cancelled out — so
                // an error that is merely returned lets the step be counted as
                // published while nothing was written.  The reference connector
                // raises and stops for the same reason.
                // Drop the step first: a snapshot batch has already set
                // `pending.resync`, and publishing it with no rows would delete the
                // endpoint's table and advance the guard over the hole.
                self.pending.step = None;
                self.fail_and_hold(e);
                return Ok(());
            }
        };
        self.pending.deltas = deltas;
        Ok(())
    }
}

impl OutputEndpoint for DuckLakeWriter {
    fn connect(&mut self, callbacks: Arc<dyn OutputEndpointCallbacks>) -> AnyResult<()> {
        self.callbacks = Some(callbacks);
        self.ensure_connected()?;
        Ok(())
    }

    fn max_buffer_size_bytes(&self) -> usize {
        usize::MAX
    }

    fn push_buffer(&mut self, _buffer: &[u8]) -> AnyResult<()> {
        unreachable!("the DuckLake connector is its own encoder")
    }

    fn push_key(
        &mut self,
        _key: Option<&[u8]>,
        _val: Option<&[u8]>,
        _headers: &[(&str, Option<&[u8]>)],
    ) -> AnyResult<()> {
        unreachable!("the DuckLake connector is its own encoder")
    }

    fn is_fault_tolerant(&self) -> bool {
        // A step whose identity the guard row already holds is a no-op, so
        // replaying it cannot duplicate it.  That is what makes at-least-once
        // delivery safe here, and it is why the guard row exists.
        true
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use feldera_types::transport::ducklake::{
        DEFAULT_ALIAS, DEFAULT_GUARD_TABLE, DEFAULT_SCHEMA, DuckLakeCatalogConfig,
        DuckLakeSinkConfig,
    };

    use super::*;

    fn config(tables: &[(&str, &str)]) -> DuckLakeWriterConfig {
        DuckLakeWriterConfig {
            catalog: DuckLakeCatalogConfig {
                catalog: "ducklake:/tmp/x.ducklake".to_owned(),
                schema: DEFAULT_SCHEMA.to_owned(),
                alias: DEFAULT_ALIAS.to_owned(),
                data_path: None,
                extension_directory: None,
                inlining_row_limit: None,
            },
            output_group: "g".to_owned(),
            generation: "gen".to_owned(),
            guard_table: DEFAULT_GUARD_TABLE.to_owned(),
            tables: tables
                .iter()
                .map(|(view, table)| {
                    (
                        (*view).to_owned(),
                        DuckLakeSinkConfig {
                            table: (*table).to_owned(),
                            key_columns: vec![],
                        },
                    )
                })
                .collect::<BTreeMap<_, _>>(),
            epoch: None,
            send_snapshot: true,
        }
    }

    fn writer(config: &DuckLakeWriterConfig, name: &str) -> DuckLakeWriter {
        DuckLakeWriter::new(
            0,
            name,
            config,
            &None,
            &Relation::empty(),
            Weak::new(),
            CancellationToken::new(),
            false,
            false,
            false,
        )
        .expect("writer")
    }

    /// An indexed view is refused at startup, like the Postgres and DynamoDB sinks
    /// do.  The sink writes plain rows; accepting the flag and ignoring it would
    /// silently change the semantics of a view that asked to be indexed, and the
    /// engine would have no way to notice.
    #[test]
    fn an_indexed_view_is_refused() {
        let refused = DuckLakeWriter::new(
            0,
            "v",
            &config(&[("v", "t")]),
            &None,
            &Relation::empty(),
            Weak::new(),
            CancellationToken::new(),
            false,
            true,
            false,
        );
        // `is_err` first: `DuckLakeWriter` is not `Debug`, so `expect_err` does not
        // apply.
        assert!(refused.is_err(), "an indexed view must be refused");
        let error = refused.err().expect("checked above");
        assert!(
            format!("{error}").contains("indexed"),
            "the error names the reason: {error}"
        );
    }

    #[test]
    fn a_single_configured_table_needs_no_view_name() {
        let w = writer(&config(&[("v", "orders")]), "out");
        assert_eq!(w.resolve_target().unwrap().table, "orders");
    }

    #[test]
    fn the_view_name_selects_the_table_when_several_are_configured() {
        let w = writer(&config(&[("v1", "a"), ("v2", "b")]), "v2");
        assert_eq!(w.resolve_target().unwrap().table, "b");
    }

    #[test]
    fn an_ambiguous_endpoint_is_refused_instead_of_guessing() {
        let w = writer(&config(&[("v1", "a"), ("v2", "b")]), "unrelated");
        assert!(w.resolve_target().is_err());
    }
}
