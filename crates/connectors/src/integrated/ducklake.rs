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
//! discharges them.  [`protocol`] states them as decisions and asserts them;
//! [`catalog`] is where they meet DuckDB.
//!
//! # Layout
//!
//! * [`ffi`] — the DuckDB C API subset the connector uses, resolved at run time
//!   so that the workspace builds without DuckDB present.
//! * [`protocol`] — batch identity, the guard row, and the total decision
//!   function.  Pure: no I/O, no connections, no HTTP, so it is testable in
//!   isolation.
//! * [`catalog`] — attaching, the guard row, and publication.  This is where a
//!   decision becomes a transaction.
//! * [`writer`] — the output endpoint: it receives a step's rows and the step's
//!   identity, and hands them to [`catalog`].
//!
//! # State of this module
//!
//! Stage B — the writer.  The reader (Stage C) is still a skeleton that fails
//! rather than silently succeeding: it bootstraps from a snapshot and then polls
//! the change feed for `start_snapshot + 1 ..= end_snapshot`, and until it does,
//! a pipeline configured with it must not report a healthy, empty source.
//!
//! # Deployment
//!
//! Two things are deliberately *not* compiled in:
//!
//! * `libduckdb`, loaded at run time from `$DUCKLAKE_DUCKDB_LIB` or the loader's
//!   search path.  Bundling it would put a C++ build in front of every
//!   `cargo build` of this workspace.
//! * the `ducklake` extension, which must be the signed binary built for exactly
//!   that DuckDB version.  `extension_directory` points at a vendored copy; on a
//!   host without egress, leaving it unset fails at `INSTALL` rather than
//!   silently at the first publication.

mod catalog;
mod ffi;
mod protocol;
mod reader;
#[cfg(test)]
mod tests_live;
mod writer;

pub use writer::DuckLakeWriter;

use anyhow::Result as AnyResult;
use feldera_adapterlib::catalog::InputCollectionHandle;
use feldera_types::config::FtModel;
use feldera_types::transport::ducklake::DuckLakeReaderConfig;

use crate::transport::{InputEndpoint, IntegratedInputEndpoint};
use crate::{InputConsumer, InputReader};

/// DuckLake input connector.
pub struct DuckLakeInputEndpoint {
    endpoint_name: String,
    config: DuckLakeReaderConfig,

    /// Where rows go once the reader can produce them.
    #[allow(dead_code)] // `consumer` is where Stage C's reader sends rows.
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
        input_handle: &InputCollectionHandle,
        resume_info: Option<serde_json::Value>,
    ) -> AnyResult<Box<dyn InputReader>> {
        reader::open(
            &self.endpoint_name,
            self.config,
            &input_handle.schema,
            input_handle,
            self.consumer,
            resume_info,
        )
    }
}
