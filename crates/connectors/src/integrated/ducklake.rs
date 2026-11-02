//! The DuckLake connector: DuckDB in-process, the catalog as the journal.
//!
//! Both halves are *integrated* connectors.  The transport is a DuckDB
//! connection and the format is DuckLake's own columnar layout, so there is no
//! boundary between "how the bytes travel" and "what the bytes mean" for a
//! separate encoder to sit on.  A DuckDB connection lives inside the pipeline
//! process, so nothing crosses a network hop that the engine would have to
//! retry, and the catalog's own transaction log is the only durable record.
//!
//! Why the protocol looks the way it does is not restated here.  The engine and
//! the catalog cannot commit together, and the fix is a single **guard row** per
//! output group, updated in the same transaction as the output it describes:
//! `publish_idempotent`, `stage_is_invisible` and `stale_epoch_is_noop` are the
//! obligations this module has to discharge, and the guard row is what
//! discharges them.
//!
//! # State of this module
//!
//! Stage A — the skeleton: configuration, registration, and the trait surface,
//! with every data path returning "not implemented".  It fails rather than
//! silently succeeding: a connector that accepted a step and wrote nothing would
//! report a healthy pipeline while losing output, which is the exact failure the
//! guard row exists to prevent.
//!
//! What the later stages have to add, since the skeleton's shape is chosen to
//! make room for it:
//!
//! * **Writer (Stage B)** attaches the catalog, reads the guard row, applies the
//!   step's deltas and the guard update in one transaction, and on failure keeps
//!   holding the output thread until shutdown.  Returning after a failed publish
//!   is what loses output, because the engine counts a returned step as
//!   processed.
//! * **Reader (Stage C)** bootstraps from a snapshot and then polls the change
//!   feed for `start_snapshot + 1 ..= end_snapshot`.  The table function's range
//!   is inclusive at both ends, so the interval has to be advanced by one rather
//!   than by zero.
//!
//! Both are reachable from here because the identity a step needs is already
//! delivered: [`Encoder::encode`] runs inside the engine's per-transaction
//! output thread, after [`OutputConsumer::batch_start`] has been told which
//! transaction the batch belongs to.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};

use anyhow::{Result as AnyResult, anyhow};
use feldera_adapterlib::catalog::{InputCollectionHandle, SerBatchReader};
use feldera_adapterlib::transport::OutputBatchType;
use feldera_types::config::FtModel;
use feldera_types::program_schema::Relation;
use feldera_types::transport::ducklake::{DuckLakeReaderConfig, DuckLakeWriterConfig};
use tokio_util::sync::CancellationToken;

use crate::controller::{ControllerInner, EndpointId};
use crate::transport::{InputEndpoint, IntegratedInputEndpoint, Step};
use crate::{
    ControllerError, Encoder, InputConsumer, InputReader, OutputConsumer, OutputEndpoint,
    OutputEndpointCallbacks,
};

/// DuckLake output connector.
///
/// Implements [`OutputConsumer`] and [`Encoder`] on one type, the way the Delta
/// Lake writer does: the step's rows and the step's identity arrive on two
/// different hooks (`push_buffer`/`batch_start`), and only something that sees
/// both can apply them in one transaction.
// Fields marked `dead_code` are read by the Stage B writer; they are here so the
// constructor takes the framework's arguments now rather than after a rewrite.
#[allow(dead_code)]
pub struct DuckLakeWriter {
    endpoint_id: EndpointId,
    endpoint_name: String,
    config: DuckLakeWriterConfig,

    /// Columns the engine writes, which is the schema the DuckLake table must
    /// have.  A table with a different one is a configuration error the writer
    /// reports, not silently adapts to.
    value_schema: Relation,

    /// Key columns when the view is an index, else `None`.
    ///
    /// Stage B uses it for keyed replacement; the protocol itself does not
    /// depend on it, because the guard row fences whole output groups rather
    /// than individual rows.
    key_schema: Option<Relation>,

    /// The controller, for reporting progress and failures.  Weak because the
    /// controller owns the endpoint.
    controller: Weak<ControllerInner>,

    /// Rows written in the batch in flight, for the endpoint's status line.
    records_written: Arc<AtomicU64>,

    /// Cancelled when the pipeline shuts down.  The writer's failure path waits
    /// on it (see the module notes), so it is held from the start.
    shutdown: CancellationToken,
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
    ) -> Result<Self, ControllerError> {
        Ok(Self {
            endpoint_id,
            endpoint_name: endpoint_name.to_string(),
            config: config.clone(),
            value_schema: value_schema.clone(),
            key_schema: key_schema.clone(),
            controller,
            records_written: Arc::new(AtomicU64::new(0)),
            shutdown,
        })
    }

    /// The error every unimplemented path returns.
    ///
    /// It names the endpoint and the output group: a mistyped configuration
    /// should say which group it meant rather than fail somewhere inside a
    /// generic "unimplemented".
    fn not_implemented(&self) -> anyhow::Error {
        anyhow!(
            "the DuckLake output connector (endpoint {}, output group {}) is not implemented yet",
            self.endpoint_name,
            self.config.output_group
        )
    }
}

impl OutputConsumer for DuckLakeWriter {
    fn max_buffer_size_bytes(&self) -> usize {
        // The writer buffers one step's rows before applying them, so it can
        // accept as much as the engine hands it.
        usize::MAX
    }

    fn batch_start(&mut self, _step: Step, _batch_type: OutputBatchType) {
        self.records_written.store(0, Ordering::Relaxed);
    }

    fn push_buffer(&mut self, _buffer: &[u8], _num_records: usize) {
        // Rows arrive as deltas, not as a serialized byte format; `encode`
        // consumes them.  Reaching this means the engine took a path the
        // connector is not wired into.
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

    fn batch_end(&mut self) {}
}

impl Encoder for DuckLakeWriter {
    fn consumer(&mut self) -> &mut dyn OutputConsumer {
        self
    }

    fn encode(&mut self, _batch: Arc<dyn SerBatchReader>) -> AnyResult<()> {
        Err(self.not_implemented())
    }
}

impl OutputEndpoint for DuckLakeWriter {
    fn connect(&mut self, _callbacks: Arc<dyn OutputEndpointCallbacks>) -> AnyResult<()> {
        todo!()
    }

    fn max_buffer_size_bytes(&self) -> usize {
        todo!()
    }

    fn push_buffer(&mut self, _buffer: &[u8]) -> AnyResult<()> {
        unreachable!()
    }

    fn push_key(
        &mut self,
        _key: Option<&[u8]>,
        _val: Option<&[u8]>,
        _headers: &[(&str, Option<&[u8]>)],
    ) -> AnyResult<()> {
        unreachable!()
    }

    fn is_fault_tolerant(&self) -> bool {
        // True once the guard row is written: a step whose identity is already
        // in the guard row is a no-op, so a replay cannot duplicate it. Stage B
        // has to make that true of the implementation rather than of the design;
        // until it does, this says so rather than guessing.
        false
    }
}

/// DuckLake input connector.
#[allow(dead_code)] // `consumer` is where Stage C's reader sends rows.
pub struct DuckLakeInputEndpoint {
    endpoint_name: String,
    config: DuckLakeReaderConfig,

    /// Where rows go once the reader can produce them.
    consumer: Box<dyn InputConsumer>,
}

impl DuckLakeInputEndpoint {
    pub fn new(
        endpoint_name: &str,
        config: &DuckLakeReaderConfig,
        consumer: Box<dyn InputConsumer>,
    ) -> Self {
        Self {
            endpoint_name: endpoint_name.to_string(),
            config: config.clone(),
            consumer,
        }
    }
}

impl InputEndpoint for DuckLakeInputEndpoint {
    fn fault_tolerance(&self) -> Option<FtModel> {
        // The change feed is replayed from a snapshot boundary rather than from
        // an engine-chosen position, so the connector cannot promise exactly
        // once; the snapshot-plus-changes read would have to be transactional
        // with the engine's checkpoint to promise that.
        Some(FtModel::AtLeastOnce)
    }
}

impl IntegratedInputEndpoint for DuckLakeInputEndpoint {
    fn open(
        self: Box<Self>,
        _input_handle: &InputCollectionHandle,
        _resume_info: Option<serde_json::Value>,
    ) -> AnyResult<Box<dyn InputReader>> {
        Err(anyhow!(
            "the DuckLake input connector (endpoint {}, table {}.{}) is not implemented yet",
            self.endpoint_name,
            self.config.catalog.schema,
            self.config.table
        ))
    }
}
