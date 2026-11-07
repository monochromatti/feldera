//! Configuration for the DuckLake connector.
//!
//! Both halves are *integrated* connectors: the transport is a DuckDB
//! connection and the data format is DuckLake's own columnar layout, so there
//! is no `format` section to configure.  Everything a deployment has to pin —
//! which catalog, which schema, which tables — is here.
//!
//! The catalog is named by one `README`-style ATTACH string rather than by
//! separate host/port/user fields.  DuckDB owns that grammar
//! (`ducklake:/path/to/meta.ducklake`,
//! `ducklake:postgres:dbname=ducklake host=...`, and whatever DuckLake adds
//! later), so re-declaring it here would be a second implementation that drifts
//! from the first.  The connector hands the string to `ATTACH` unchanged.
//!
//! Publication needs a protected row per output group — the guard row — because
//! the engine and the catalog cannot commit together; see the protocol notes in
//! `crates/connectors/src/integrated/ducklake.rs`.  Its shape is fixed:
//!
//! ```text
//! (output_group, generation, epoch, frontier, batch_id, digest, updated_at)
//! ```
//!
//! with `output_group` unique.  `guard_table` and the catalog schema name it.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::duration::ConfigDuration;

/// Table holding one guard row per output group, in the catalog's schema.
///
/// The name is a configuration key rather than a constant because two pipelines
/// may share a catalog; they then either use the same guard table with different
/// `output_group`s or separate tables, and neither choice is ours to make.
pub const DEFAULT_GUARD_TABLE: &str = "connector_guard";

/// DuckLake schema that holds the connector's tables and its guard table.
pub const DEFAULT_SCHEMA: &str = "main";

/// Name under which the catalog is attached on the connector's connection.
///
/// Short and fixed so that the SQL the connector builds stays readable in logs;
/// it is never user-visible in a way that matters, but `ATTACH` needs a name and
/// a hard-coded one keeps two statements from disagreeing.
pub const DEFAULT_ALIAS: &str = "dl";

/// How often the input connector asks the catalog for new changes.
///
/// DuckLake exposes changes as a table function over a snapshot range, not as a
/// subscription, so the connector polls.  One second is the default because the
/// cost of a poll that finds nothing is a single query against the catalog, and
/// the alternative — a longer interval — shows up directly as end-to-end
/// latency.
pub const DEFAULT_POLL_INTERVAL: ConfigDuration = ConfigDuration::from_secs(1);

fn default_schema() -> String {
    DEFAULT_SCHEMA.to_string()
}

fn default_alias() -> String {
    DEFAULT_ALIAS.to_string()
}

fn default_guard_table() -> String {
    DEFAULT_GUARD_TABLE.to_string()
}

/// Where a DuckLake catalog lives and how to attach it.
///
/// Shared by both halves of the connector and flattened into each of their
/// configs, so a writer config looks like this:
///
/// ```json
/// {
///   "catalog": "ducklake:/var/lib/feldera/meta.ducklake",
///   "schema": "main",
///   "output_group": "orders",
///   "generation": "v1",
///   "tables": { "orders_out": { "table": "orders" } }
/// }
/// ```
#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct DuckLakeCatalogConfig {
    /// ATTACH target for the DuckLake catalog.
    ///
    /// Either a metadata file (`ducklake:/path/to/meta.ducklake`) or a
    /// PostgreSQL-backed catalog (`ducklake:postgres:dbname=ducklake host=...`).
    /// The form decides how concurrent access fails, and the difference matters:
    /// a file-backed catalog refuses a second process the `ATTACH` itself, while
    /// a PostgreSQL-backed one admits two writers and reports the conflict at
    /// `COMMIT`. The connector is correct under both, but only the latter is a
    /// configuration where two processes can actually race.
    pub catalog: String,

    /// DuckLake schema holding the connector's tables.
    ///
    /// Default: `main`.
    #[serde(default = "default_schema")]
    pub schema: String,

    /// Name the catalog is attached under.
    ///
    /// Default: `dl`.
    #[serde(default = "default_alias")]
    pub alias: String,

    /// Directory that DuckLake writes data files to, overriding the catalog's
    /// own setting.
    ///
    /// Needed when the catalog was created with a data path that is not valid
    /// from the pipeline host. Unset uses the catalog's.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data_path: Option<String>,

    /// Directory that holds the DuckLake extension binary.
    ///
    /// Set it to the directory holding a vendored `ducklake.duckdb_extension`;
    /// unset lets DuckDB download the extension on first use, which fails on a
    /// host without egress. Deployment should vendor the file: extension
    /// binaries are per-platform and version-locked to the DuckDB library, so
    /// the two are pinned together.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extension_directory: Option<String>,

    /// Rows per data file stored inline in the catalog rather than written to a
    /// separate file.
    ///
    /// Unset uses DuckLake's default. Lowering it makes the catalog the
    /// bottleneck sooner; raising it makes every commit carry more payload.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inlining_row_limit: Option<u64>,
}

/// One Feldera output view and the DuckLake table its rows land in.
#[derive(Debug, Clone, Default, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct DuckLakeSinkConfig {
    /// DuckLake table to write the view's rows to.
    ///
    /// Created by the catalog owner, not by the connector: the connector's job is
    /// to keep one output group's frontier and its rows in step, and a table it
    /// invented on the fly would have no such history.
    pub table: String,

    /// Columns that identify a row, for keyed replacement.
    ///
    /// A step carries signed multiplicities, not a keyed diff, so a row that the
    /// view removed and re-added is a `-1` and a `+1` of the same row. With key
    /// columns the connector replaces by key (`DELETE` where the key matches,
    /// then `INSERT`); without them it deletes exactly one matching row and
    /// inserts one, which preserves a bag. Unset keeps the bag semantics, which
    /// is right for every table that is not known to have a key.
    ///
    /// **This is a promise about the data, not a hint.** Keyed replacement removes
    /// at most one row per key, so if the view can produce two rows with the same
    /// key, the second insert replaces the first and the table ends up holding one
    /// row where the pipeline emitted two — an undercount the guard cannot see,
    /// because the guard records that the step was applied, not what it applied.
    /// The connector cannot check the promise: it would have to read the view's
    /// distinct-key count, which is unbounded work on every step. Leave this unset
    /// unless the view is known to be unique on these columns.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub key_columns: Vec<String>,
}

/// DuckLake output connector configuration.
#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct DuckLakeWriterConfig {
    #[serde(flatten)]
    pub catalog: DuckLakeCatalogConfig,

    /// Name of the output group this connector publishes.
    ///
    /// One guard row per group, so the group name is what a second pipeline must
    /// not reuse for a different set of tables: two publishers that share a group
    /// fence each other off instead of interleaving writes into the same rows.
    pub output_group: String,

    /// Identity of this publication arrangement, e.g. a pipeline generation.
    ///
    /// Part of every batch identity, so a re-created pipeline that inherits the
    /// group's guard row must keep the generation it had, or take over
    /// explicitly with a higher `epoch`. A changed generation under an unchanged
    /// epoch is refused rather than guessed at.
    pub generation: String,

    /// Table holding this group's guard row.
    ///
    /// Default: `connector_guard`.
    #[serde(default = "default_guard_table")]
    pub guard_table: String,

    /// Feldera output view to DuckLake table mapping, for one step.
    ///
    /// Ordered by view name so that the assembly order — and therefore the SQL
    /// the connector emits, and the digest it computes — is a property of the
    /// configuration rather than of a hash map's iteration order.
    pub tables: BTreeMap<String, DuckLakeSinkConfig>,

    /// Ownership epoch, used to take over a group that a previous owner pinned.
    ///
    /// Must strictly increase across takeovers; the connector refuses to write at
    /// an epoch that is not greater than the one in the guard row. Unset keeps
    /// the row's epoch and fails if this generation did not pin it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub epoch: Option<u64>,

    /// Whether to resynchronize from the engine's snapshot when the guard row is
    /// behind the tables.
    ///
    /// Default: `true`. Disabling it turns a catalog that is out of step into a
    /// startup error instead of a rewrite, which is what a deployment that
    /// suspects the two disagree wants.
    ///
    /// **Spell this key exactly.** The catalog block is `#[serde(flatten)]`ed, and
    /// `deny_unknown_fields` cannot be combined with `flatten`, so a misspelled key
    /// is silently ignored and this default applies — a `send_snapshott: false`
    /// deployment would get the rewrite it asked not to have. The connector cannot
    /// detect the typo, so the spelling is the contract.
    #[serde(default = "default_true", skip_serializing_if = "is_true")]
    pub send_snapshot: bool,
}

/// DuckLake input connector configuration.
#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct DuckLakeReaderConfig {
    #[serde(flatten)]
    pub catalog: DuckLakeCatalogConfig,

    /// DuckLake table to read.
    ///
    /// Read as a change stream: the first read is a bootstrap snapshot of the
    /// table's current contents, and every later read asks for the changes since
    /// the last one. Both are the same table function over a snapshot range, so
    /// the connector has no notion of a "full" versus an "incremental" query to
    /// get wrong.
    pub table: String,

    /// How often to ask the catalog for new changes, for example `500ms`.
    ///
    /// Unset selects [`DEFAULT_POLL_INTERVAL`]. The interval is the floor on
    /// end-to-end latency, not a rate limit, since a poll that finds nothing
    /// costs one query.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Option<String>)]
    pub poll_interval: Option<ConfigDuration>,
}

impl DuckLakeReaderConfig {
    /// The poll interval in effect, or [`DEFAULT_POLL_INTERVAL`].
    pub fn poll_interval(&self) -> ConfigDuration {
        self.poll_interval.unwrap_or(DEFAULT_POLL_INTERVAL)
    }
}

fn default_true() -> bool {
    true
}

fn is_true(value: &bool) -> bool {
    *value
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A writer config written the way a user writes it — flat, with the catalog
    /// fields at the top level — round-trips to the same JSON. This is what
    /// `openapi.json` publishes, and what a client generated from it sends.
    #[test]
    fn writer_config_round_trips_through_json() {
        let json = r#"{
            "catalog": "ducklake:/var/lib/feldera/meta.ducklake",
            "output_group": "orders",
            "generation": "v1",
            "tables": {
                "orders_out": { "table": "orders", "key_columns": ["id"] }
            }
        }"#;
        let config: DuckLakeWriterConfig = serde_json::from_str(json).unwrap();

        assert_eq!(config.catalog.schema, DEFAULT_SCHEMA);
        assert_eq!(config.catalog.alias, DEFAULT_ALIAS);
        assert_eq!(config.guard_table, DEFAULT_GUARD_TABLE);
        assert!(
            config.send_snapshot,
            "resynchronizing is the default, so that a catalog behind the engine is repaired"
        );
        assert_eq!(
            config.tables["orders_out"].key_columns,
            vec!["id".to_string()]
        );

        // Defaults stay out of the serialized form, so an older runtime reading
        // a newer config does not see keys it does not know.
        let serialized = serde_json::to_value(&config).unwrap();
        assert!(serialized.get("send_snapshot").is_none());
        assert!(serialized.get("epoch").is_none());
        assert_eq!(serialized["guard_table"], DEFAULT_GUARD_TABLE);
        assert_eq!(
            serialized["catalog"],
            "ducklake:/var/lib/feldera/meta.ducklake"
        );

        let reparsed: DuckLakeWriterConfig = serde_json::from_value(serialized).unwrap();
        assert_eq!(reparsed, config);
    }

    /// The reader's poll interval accepts a duration string and defaults when
    /// unset, because both spellings appear in examples and either one alone
    /// would be a trap.
    #[test]
    fn reader_poll_interval_has_a_default_and_parses() {
        let config: DuckLakeReaderConfig =
            serde_json::from_str(r#"{"catalog": "ducklake:/x/meta.ducklake", "table": "orders"}"#)
                .unwrap();
        assert_eq!(config.poll_interval(), DEFAULT_POLL_INTERVAL);

        let config: DuckLakeReaderConfig = serde_json::from_str(
            r#"{"catalog": "ducklake:/x/meta.ducklake", "table": "orders", "poll_interval": "250ms"}"#,
        )
        .unwrap();
        assert_eq!(
            config.poll_interval(),
            ConfigDuration::from_millis(250),
            "a duration string is accepted, as elsewhere in the connector configs"
        );
    }
}
