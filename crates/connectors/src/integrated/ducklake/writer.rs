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

use super::catalog::{Catalog, redact_catalog};
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
    ///
    /// `fail_and_hold` reports through it, because an integrated endpoint is
    /// never handed `connect` and so never receives `callbacks`.
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

    /// Set when a step was refused.  A refused step must not be followed by a
    /// successful call, or the controller can count it as processed.
    poisoned: Option<String>,
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
        let mut writer = Self {
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
            poisoned: None,
        };
        // Resolved here rather than on the first step.  A `tables` mapping that
        // names no view, or more than one, is a configuration error, and finding it
        // in `encode` meant the output thread held until shutdown while the pipeline
        // looked merely idle.  The catalog itself is still attached on first use: an
        // integrated endpoint never gets `connect`, and attaching at construction
        // would hold a DuckDB attachment, and its file lock, from pipeline start.
        writer.target = Some(writer.resolve_target().map_err(|e| {
            ControllerError::invalid_transport_configuration(endpoint_name, &e.to_string())
        })?);
        Ok(writer)
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
        let catalog = Catalog::open(&self.config)?;
        tracing::info!(
            "ducklake: {} attached {:?} at frontier {} epoch {}",
            self.endpoint_name,
            redact_catalog(&self.config.catalog.catalog),
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
            Err(e) => {
                let refused = self.fail_and_hold(e);
                // The hold ends only at shutdown, and `batch_end` has no way to
                // report failure, so the refusal has to surface on the next call
                // the engine makes — see `poisoned` and `encode`.
                self.poisoned = Some(format!("{refused:#}"));
            }
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
    /// whose output is not in the catalog as processed.  Holding alone is not
    /// enough, though — shutdown cancels the hold, so the error is returned too:
    /// a caller that reports success after a refused publication hands the
    /// controller exactly the "processed but never committed" step this module
    /// exists to prevent.
    fn fail_and_hold(&mut self, error: anyhow::Error) -> anyhow::Error {
        let detail = format!("{error:#}");
        tracing::error!(
            "ducklake: {} failed to publish: {detail}",
            self.endpoint_name
        );
        let report = anyhow!("the DuckLake connector could not publish a step: {detail}");
        if let Some(callbacks) = &self.callbacks {
            callbacks.error(true, report, Some("ducklake_publication"));
        } else if let Some(controller) = self.controller.upgrade() {
            // An integrated endpoint is never handed `connect` (the controller takes
            // `into_encoder` and never calls it), so `callbacks` is `None` for every
            // writer the engine creates.  Without this branch a failed publication is
            // a log line and a thread that holds forever: no `output_transport_error`,
            // no error count, no health change — and the error returned when shutdown
            // ends the hold is downgraded to a debug log by the controller.  Reporting
            // here is what turns it into something an operator can see.
            controller.output_transport_error(
                self.endpoint_id,
                &self.endpoint_name,
                true,
                report,
                Some("ducklake_publication"),
            );
        }
        // Hold until the pipeline tears down.  The sleep bounds the latency of
        // noticing the cancellation, and the token is the only thing that can
        // end this loop.
        while !self.shutdown.is_cancelled() {
            std::thread::sleep(HOLD_POLL);
        }
        error
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
pub(super) const MAX_ATTEMPTS: usize = 8;

/// How often the failure path checks for shutdown while it holds the thread.
const HOLD_POLL: Duration = Duration::from_millis(200);

/// Largest row multiplicity expanded in place.
const MAX_MULTIPLICITY: i64 = 1024;

impl OutputConsumer for DuckLakeWriter {
    fn max_buffer_size_bytes(&self) -> usize {
        // Rows are read from the engine's own batch representation, so there is no
        // *transport* buffer to bound.  The claim stops there: `Pending::deltas`
        // holds every row of the step in memory until `batch_end`, because a step
        // is published in one transaction and there is no cursor-friendly way to
        // build it incrementally.  Bounding that here would only make the engine
        // split a step it cannot split.
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
                    // Clear the step in flight: the hold runs while the engine keeps
                    // calling the encoder, and a stale step would attribute this
                    // snapshot's rows to the previous step's identity.
                    self.pending.step = None;
                    self.poisoned = Some(format!(
                        "the engine sent a snapshot for {} but `send_snapshot` is false",
                        self.endpoint_name
                    ));
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
        // A refused step is not republishable: say so rather than accepting work
        // that can never be published behind it.
        if let Some(refused) = &self.poisoned {
            bail!("the DuckLake writer refused an earlier step and accepts no more: {refused}");
        }

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
                // Return the error after the hold, so the failure is not reported as
                // a successful step once shutdown ends the hold.
                return Err(self.fail_and_hold(e));
            }
        };
        self.pending.deltas.extend(deltas);
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
        // The trait asks for an error here, not a panic: "If a given transport does
        // not implement this API, it should return an error".  Nothing calls this
        // today — the integrated path drives the `OutputConsumer` methods — but
        // `IntegratedOutputEndpoint::as_endpoint()` exposes this trait, and a panic
        // on that path would take down the output thread.
        bail!("the DuckLake connector is its own encoder and takes no byte buffers")
    }

    fn push_key(
        &mut self,
        _key: Option<&[u8]>,
        _val: Option<&[u8]>,
        _headers: &[(&str, Option<&[u8]>)],
    ) -> AnyResult<()> {
        bail!("the DuckLake connector is its own encoder and takes no keyed byte records")
    }

    fn is_fault_tolerant(&self) -> bool {
        // A step whose identity the guard row already holds is a no-op, so
        // replaying it cannot duplicate it.  That is what makes at-least-once
        // delivery safe here, and it is why the guard row exists.  A refused step
        // is the one thing that is *not* safe to checkpoint, so poisoning the
        // writer withdraws the claim.
        self.poisoned.is_none()
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
        // Refused at construction, not on the first step: this used to be found in
        // `ensure_connected`, i.e. inside the output thread, which then held until
        // shutdown while the pipeline looked merely idle.  The requirement is
        // unchanged — refuse rather than guess — so the assertion moved rather than
        // the expectation.
        let refused = DuckLakeWriter::new(
            0,
            "unrelated",
            &config(&[("v1", "a"), ("v2", "b")]),
            &None,
            &Relation::empty(),
            Weak::new(),
            CancellationToken::new(),
            false,
            false,
            false,
        );
        assert!(
            refused.is_err(),
            "an endpoint that names no configured view must be refused, not guessed at"
        );
        let error = refused.err().expect("checked above");
        assert!(
            format!("{error}").contains("cannot tell which"),
            "the error names the ambiguity: {error}"
        );
    }
}
