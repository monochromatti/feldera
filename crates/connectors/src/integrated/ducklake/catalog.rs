//! The catalog as the writer sees it: attach, the guard row, and publication.
//!
//! The counterpart of `catalog.py` in the Python bridge, minus everything the
//! reader needs (snapshots and the change feed belong to Stage C).
//!
//! The publication shape is the one the Python layer settled on and the formal
//! witnesses describe: *one* DuckLake transaction that both applies the step's
//! payload and advances the guard row, guarded by a conditional `UPDATE` whose
//! `WHERE` clause carries the frontier and epoch that were read.  A publisher
//! that lost a write-write race updates zero rows, and the re-read turns that
//! into an error rather than a silent double apply.
//!
//! A transaction that loses the race is retried with the *same* batch identity.
//! That is what makes a retry after a lost acknowledgement safe: the retry
//! either applies (the guard had not moved) or is recognised as a duplicate (it
//! had already committed) — never a second apply.

use std::collections::BTreeMap;

use anyhow::{Context, Result, anyhow, bail};
use feldera_types::transport::ducklake::{DuckLakeCatalogConfig, DuckLakeWriterConfig};

use super::ffi::{Conn, Db, Library, Value};
use super::protocol::{self, Batch, Decision, Delta, Guard, Op};

/// Outcome of one publication (possibly after retries).
#[derive(Debug)]
pub struct PublicationResult {
    pub decision: Decision,
    pub guard: Guard,
    pub attempts: usize,
}

/// True when a DuckDB error is a write-write conflict that may be retried.
///
/// DuckLake reports the loser of a concurrent commit as a transaction-context
/// failure rather than with a distinct error code, so the message is all there
/// is to match on.  The needles are the same ones the Python layer uses, which
/// is why they are listed rather than inferred.
fn is_transaction_conflict(message: &str) -> bool {
    let lower = message.to_lowercase();
    [
        "conflict",
        "transactioncontext",
        "failed to commit",
        "optimistic",
        "write-write",
    ]
    .iter()
    .any(|needle| lower.contains(needle))
}

/// Quote an identifier, refusing anything that is not a plain name.
///
/// Configuration supplies these names, and they are interpolated into SQL, so a
/// name that is not a plain identifier is rejected instead of quoted and hoped
/// for.
pub fn quote_ident(ident: &str) -> Result<String> {
    let mut chars = ident.chars();
    let valid = match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {
            chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
        }
        _ => false,
    };
    if !valid {
        bail!("unsafe SQL identifier {ident:?}");
    }
    Ok(format!("\"{ident}\""))
}

/// A DuckLake catalog plus the publication protocol over it.
pub struct Catalog {
    #[allow(dead_code)] // the library stays loaded for the catalog's lifetime
    lib: &'static Library,
    /// Owns the DuckDB database handle.  Never read after `open`.  Declared
    /// **after** `conn` on purpose: fields are dropped in declaration order, and
    /// `duckdb_close` requires every connection to be closed first — with this
    /// last, `duckdb_disconnect` runs before the database is closed.
    #[allow(dead_code)]
    db: Db<'static>,
    conn: Conn<'static>,
    alias: String,
    schema: String,
    guard_table: String,
    output_group: String,
    generation: String,
    epoch: u64,
    /// DuckLake table -> key columns, for keyed replacement.
    keys: BTreeMap<String, Vec<String>>,
    /// Tables this connector owns; a batch writing anything else is rejected.
    tables: Vec<String>,
}

impl Catalog {
    /// Attach the catalog and make the guard row exist.
    ///
    /// Ownership is settled here rather than at publication time: a writer that
    /// cannot own its group must not start consuming steps, because every step
    /// it consumes is one it cannot publish.
    pub fn open(config: &DuckLakeWriterConfig) -> Result<Catalog> {
        let lib = Library::load()?;

        let cdc = &config.catalog;
        let db_config = lib.config()?;
        if let Some(dir) = &cdc.extension_directory {
            lib.set_extension_directory(*db_config, dir)?;
        }
        let db = Db::open(lib, ":memory:", *db_config).with_context(|| {
            format!(
                "opening the DuckDB connection for catalog {:?}",
                cdc.catalog
            )
        })?;
        let conn = db.connect()?;

        // `LOAD` before `INSTALL`: on a host with egress the extension may not be
        // there yet, but when it is (a vendored `extension_directory`) this avoids
        // a download attempt.
        if conn.query("LOAD ducklake", &[]).is_err() {
            conn.execute("INSTALL ducklake", &[]).with_context(|| {
                "installing the ducklake extension (set extension_directory to a vendored \
                 ducklake.duckdb_extension on a host without egress)"
            })?;
            conn.execute("LOAD ducklake", &[])?;
        }

        conn.execute(&attach_statement(cdc, false)?, &[])
            .with_context(|| format!("attaching catalog {:?}", cdc.catalog))?;

        let mut keys = BTreeMap::new();
        let mut tables = Vec::new();
        for sink in config.tables.values() {
            quote_ident(&sink.table)?;
            if !keys.contains_key(&sink.table) {
                tables.push(sink.table.clone());
                keys.insert(sink.table.clone(), sink.key_columns.clone());
            }
        }
        tables.sort();

        let mut catalog = Catalog {
            lib,
            db,
            alias: cdc.alias.clone(),
            schema: cdc.schema.clone(),
            guard_table: config.guard_table.clone(),
            output_group: config.output_group.clone(),
            generation: config.generation.clone(),
            epoch: config.epoch.unwrap_or(1),
            keys,
            tables,
            conn,
        };
        catalog.create_guard_table()?;
        catalog.settle_ownership(config.epoch)?;
        Ok(catalog)
    }

    /// The frontier in the guard row.
    ///
    /// This used to default to 0 when the read failed, which turns a broken
    /// connection into "this output group has published nothing" — the writer would
    /// then base every batch on 0 and the protocol would call them duplicates.
    pub fn frontier(&self) -> Result<u64> {
        Ok(self.read_guard()?.frontier)
    }

    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    fn qualified(&self, table: &str) -> Result<String> {
        Ok(format!(
            "{}.{}.{}",
            quote_ident(&self.alias)?,
            quote_ident(&self.schema)?,
            quote_ident(table)?
        ))
    }

    fn guard_table_sql(&self) -> Result<String> {
        self.qualified(&self.guard_table)
    }

    fn create_guard_table(&mut self) -> Result<()> {
        let sql = format!(
            "CREATE TABLE IF NOT EXISTS {} (\
               output_group VARCHAR NOT NULL, \
               generation VARCHAR NOT NULL, \
               epoch BIGINT NOT NULL, \
               frontier BIGINT NOT NULL, \
               batch_id VARCHAR, \
               digest VARCHAR, \
               updated_at TIMESTAMP)",
            self.guard_table_sql()?
        );
        self.conn.execute(&sql, &[])?;
        Ok(())
    }

    /// Provision the guard row if absent, then reconcile generation and epoch.
    fn settle_ownership(&mut self, requested_epoch: Option<u64>) -> Result<()> {
        let guard = match self.read_guard() {
            Ok(guard) => guard,
            Err(e) if format!("{e}").contains("exactly one guard row") => self.provision_guard()?,
            Err(e) => return Err(e),
        };
        if guard.generation != self.generation {
            bail!(
                "output group {:?} belongs to generation {:?}, not {:?}; a new pipeline \
                 generation requires an explicit reset so that stale results are not mixed with \
                 the new run",
                self.output_group,
                guard.generation,
                self.generation
            );
        }
        match requested_epoch {
            None => self.epoch = guard.epoch,
            Some(epoch) if epoch > guard.epoch => {
                let taken = self.take_over(epoch)?;
                self.epoch = taken.epoch;
            }
            Some(epoch) if epoch < guard.epoch => bail!(
                "epoch {epoch} is fenced: output group {:?} is owned at epoch {}",
                self.output_group,
                guard.epoch
            ),
            Some(_) => self.epoch = guard.epoch,
        }
        Ok(())
    }

    pub fn read_guard(&self) -> Result<Guard> {
        let sql = format!(
            "SELECT output_group, generation, epoch, frontier, batch_id, digest FROM {} \
             WHERE output_group = ?",
            self.guard_table_sql()?
        );
        let rows = self
            .conn
            .query(&sql, &[Value::Text(self.output_group.clone())])?;
        let rows = rows.into_rows();
        if rows.len() != 1 {
            bail!(
                "output group {:?} must have exactly one guard row, found {}",
                self.output_group,
                rows.len()
            );
        }
        let row = &rows[0];
        let cell = |i: usize| row.get(i).cloned().flatten();
        // Parsed rather than defaulted: a NULL or non-numeric cell used to become
        // 0, which then made the conditional guard `UPDATE` match no row and
        // surface as "the guard did not advance" — the right refusal, with a
        // cause that sent the operator looking in the wrong place.
        let number = |i: usize, column: &str| -> Result<u64> {
            let text = cell(i).with_context(|| {
                format!("guard row of {:?} has no {column}", self.output_group)
            })?;
            text.parse().with_context(|| {
                format!(
                    "guard row of {:?} has a non-numeric {column}: {text:?}",
                    self.output_group
                )
            })
        };
        Ok(Guard {
            output_group: cell(0).unwrap_or_default(),
            generation: cell(1).unwrap_or_default(),
            epoch: number(2, "epoch")?,
            frontier: number(3, "frontier")?,
            batch_id: cell(4),
            digest: cell(5),
        })
    }

    fn provision_guard(&mut self) -> Result<Guard> {
        let guard = Guard::initial(self.output_group.clone(), self.generation.clone());
        let sql = format!(
            "INSERT INTO {} (output_group, generation, epoch, frontier, batch_id, digest, \
             updated_at) SELECT ?, ?, ?, ?, ?, ?, now() WHERE NOT EXISTS (SELECT 1 FROM {} \
             WHERE output_group = ?)",
            self.guard_table_sql()?,
            self.guard_table_sql()?
        );
        self.conn.execute(
            &sql,
            &[
                Value::Text(guard.output_group.clone()),
                Value::Text(guard.generation.clone()),
                Value::Int(guard.epoch as i64),
                Value::Int(guard.frontier as i64),
                Value::Null,
                Value::Null,
                Value::Text(guard.output_group.clone()),
            ],
        )?;
        self.read_guard()
    }

    /// Bump the ownership epoch in a transaction of its own.
    ///
    /// Publishers holding an older epoch are then fenced by the conditional
    /// guard update, not by a lock: their `UPDATE` matches no row.
    pub fn take_over(&mut self, epoch: u64) -> Result<Guard> {
        let guard = self.read_guard()?;
        let next = protocol::acquire_ownership(&guard, epoch)?;
        let sql = format!(
            "UPDATE {} SET epoch = ?, updated_at = now() WHERE output_group = ? AND epoch = ?",
            self.guard_table_sql()?
        );
        self.conn.execute("BEGIN TRANSACTION", &[])?;
        let result = self.conn.execute(
            &sql,
            &[
                Value::Int(next.epoch as i64),
                Value::Text(self.output_group.clone()),
                Value::Int(guard.epoch as i64),
            ],
        );
        if let Err(e) = result {
            let _ = self.conn.execute("ROLLBACK", &[]);
            return Err(e);
        }
        let current = self.read_guard()?;
        if current.epoch != next.epoch {
            let _ = self.conn.execute("ROLLBACK", &[]);
            bail!(
                "takeover of {:?} to epoch {epoch} did not take effect",
                self.output_group
            );
        }
        if let Err(e) = self.conn.execute("COMMIT", &[]) {
            let _ = self.conn.execute("ROLLBACK", &[]);
            return Err(e);
        }
        Ok(current)
    }

    /// Apply `batch` atomically with the guard update, or explain why not.
    pub fn publish(&mut self, batch: &Batch, max_attempts: usize) -> Result<PublicationResult> {
        protocol::validate(batch, &self.tables)?;
        let mut conflict: Option<String> = None;
        for attempt in 1..=max_attempts.max(1) {
            if let Err(e) = self.conn.execute("BEGIN TRANSACTION", &[]) {
                if is_transaction_conflict(&format!("{e:#}")) {
                    conflict = Some(format!("{e:#}"));
                    continue;
                }
                return Err(e);
            }
            let outcome = self.attempt(batch);
            let result = match outcome {
                Ok(result) => result,
                Err(e) => {
                    let message = format!("{e:#}");
                    let _ = self.conn.execute("ROLLBACK", &[]);
                    if !is_transaction_conflict(&message) || attempt >= max_attempts.max(1) {
                        return Err(e);
                    }
                    conflict = Some(message);
                    continue;
                }
            };
            // `COMMIT` is part of the attempt, not a formality after it.  DuckLake
            // detects a write-write race at commit — that is what an optimistic
            // catalog commit is — so retrying only the statements before it would
            // turn the most likely conflict into a hard error, which the caller
            // cannot distinguish from a real failure.
            match self.conn.execute("COMMIT", &[]) {
                Ok(()) => {
                    return Ok(PublicationResult {
                        guard: protocol::advance(&result.guard, batch),
                        decision: result.decision,
                        attempts: attempt,
                    });
                }
                Err(e) => {
                    let message = format!("{e:#}");
                    // A failed `COMMIT` may leave the transaction open; without this
                    // the next `BEGIN` is a no-op inside it and later statements
                    // pile into one transaction that a later `ROLLBACK` discards.
                    let _ = self.conn.execute("ROLLBACK", &[]);
                    if !is_transaction_conflict(&message) || attempt >= max_attempts.max(1) {
                        return Err(e);
                    }
                    conflict = Some(message);
                }
            }
        }
        bail!(
            "publication of batch {} did not converge: {}",
            batch.batch_id(),
            conflict.unwrap_or_default()
        )
    }

    /// One attempt inside an open transaction.
    fn attempt(&mut self, batch: &Batch) -> Result<Attempt> {
        let guard = self.read_guard()?;
        let decision = protocol::decide(&guard, batch)?;
        if decision != Decision::Apply {
            return Ok(Attempt { decision, guard });
        }
        self.update_guard(&guard, batch)?;
        self.apply_payload(batch)?;
        Ok(Attempt { decision, guard })
    }

    /// Conditional guarded update; writes nothing if another writer won.
    fn update_guard(&mut self, guard: &Guard, batch: &Batch) -> Result<()> {
        let sql = format!(
            "UPDATE {} SET frontier = ?, batch_id = ?, digest = ?, updated_at = now() \
             WHERE output_group = ? AND generation = ? AND epoch = ? AND frontier = ?",
            self.guard_table_sql()?
        );
        self.conn.execute(
            &sql,
            &[
                Value::Int(batch.end as i64),
                Value::Text(batch.batch_id()),
                Value::Text(batch.digest()),
                Value::Text(guard.output_group.clone()),
                Value::Text(guard.generation.clone()),
                Value::Int(guard.epoch as i64),
                Value::Int(guard.frontier as i64),
            ],
        )?;
        let current = self.read_guard()?;
        if current.frontier != batch.end
            || current.batch_id.as_deref() != Some(batch.batch_id().as_str())
        {
            bail!(
                "guard for {:?} did not advance to {}; a concurrent publisher changed it",
                guard.output_group,
                batch.end
            );
        }
        Ok(())
    }

    fn apply_payload(&mut self, batch: &Batch) -> Result<()> {
        if batch.resync {
            for table in &self.tables {
                let sql = format!("DELETE FROM {}", self.qualified(table)?);
                self.conn.execute(&sql, &[])?;
            }
        }
        // Deletions first: in bag mode a delete must match the state the batch
        // starts from, which still contains rows this batch also inserts.
        let mut ordered: Vec<&Delta> = batch.deltas.iter().collect();
        ordered.sort_by_key(|d| (d.op == Op::Insert, d.canonical()));
        for delta in ordered {
            self.apply_delta(delta)?;
        }
        Ok(())
    }

    fn apply_delta(&mut self, delta: &Delta) -> Result<()> {
        let target = self.qualified(&delta.table)?;
        let row = match &delta.row {
            serde_json::Value::Object(map) => map,
            other => bail!("delta row is not an object: {other}"),
        };
        let mut columns: Vec<&String> = row.keys().collect();
        columns.sort();

        if let Some(keys) = self.keys.get(&delta.table).filter(|k| !k.is_empty()) {
            let predicate = keys
                .iter()
                .map(|k| Ok(format!("{} = ?", quote_ident(k)?)))
                .collect::<Result<Vec<_>>>()?
                .join(" AND ");
            let params: Result<Vec<Value>> = keys
                .iter()
                .map(|k| {
                    row.get(k)
                        .map(Value::from_json)
                        .ok_or_else(|| anyhow!("row on {:?} has no key column {k:?}", delta.table))
                })
                .collect();
            let sql = format!("DELETE FROM {target} WHERE {predicate}");
            self.conn.execute(&sql, &params?)?;
        } else if delta.op == Op::Delete {
            // Multiplicity-preserving removal: exactly one matching row.
            let predicate = columns
                .iter()
                .map(|c| Ok(format!("{} IS NOT DISTINCT FROM ?", quote_ident(c)?)))
                .collect::<Result<Vec<_>>>()?
                .join(" AND ");
            let params: Vec<Value> = columns
                .iter()
                .map(|c| Value::from_json(row.get(*c).expect("column comes from the row")))
                .collect();
            let sql = format!(
                "DELETE FROM {target} WHERE rowid IN (SELECT rowid FROM {target} WHERE \
                 {predicate} LIMIT 1)"
            );
            self.conn.execute(&sql, &params)?;
        }

        if delta.op == Op::Insert {
            let names = columns
                .iter()
                .map(|c| quote_ident(c))
                .collect::<Result<Vec<_>>>()?
                .join(", ");
            let placeholders = vec!["?"; columns.len()].join(", ");
            let params: Vec<Value> = columns
                .iter()
                .map(|c| Value::from_json(row.get(*c).expect("column comes from the row")))
                .collect();
            let sql = format!("INSERT INTO {target} ({names}) VALUES ({placeholders})");
            self.conn.execute(&sql, &params)?;
        }
        Ok(())
    }

    #[allow(dead_code)]
    fn library_version(&self) -> String {
        self.lib.version()
    }
}

struct Attempt {
    decision: Decision,
    guard: Guard,
}

/// The `ATTACH` statement for a catalog, which the reader and the writer share.
///
/// `read_only` is what makes a reader safe: it cannot write the guard row, an
/// output table, or anything else, whatever the code above it does.
pub(crate) fn attach_statement(cdc: &DuckLakeCatalogConfig, read_only: bool) -> Result<String> {
    let mut options = Vec::new();
    if read_only {
        options.push("READ_ONLY".to_string());
    }
    if let Some(data_path) = &cdc.data_path {
        options.push(format!("DATA_PATH {}", sql_literal(data_path)?));
    }
    if let Some(limit) = cdc.inlining_row_limit {
        options.push(format!("DATA_INLINING_ROW_LIMIT {limit}"));
    }
    let suffix = if options.is_empty() {
        String::new()
    } else {
        format!(" ({})", options.join(", "))
    };
    Ok(format!(
        "ATTACH {} AS {}{suffix}",
        sql_literal(&cdc.catalog)?,
        quote_ident(&cdc.alias)?
    ))
}

/// Render a string as a SQL literal.
///
/// `ATTACH` takes its target and `DATA_PATH` as literals rather than as bound
/// parameters, so the value is quoted here with the standard doubling rule.
pub(crate) fn sql_literal(value: &str) -> Result<String> {
    if value.contains('\0') {
        bail!("SQL literal contains a NUL byte: {value:?}");
    }
    Ok(format!("'{}'", value.replace('\'', "''")))
}

#[cfg(any(test, feature = "testing"))]
impl Catalog {
    /// Run a statement for a test that needs a table to exist.
    ///
    /// The connector never creates output tables — they are the catalog owner's,
    /// because a table it invented would have no publication history — so the
    /// tests seed them here instead.
    pub(crate) fn execute_sql(&mut self, sql: &str) -> Result<()> {
        self.conn.execute(sql, &[])
    }

    pub(crate) fn scalar(&mut self, sql: &str) -> Result<Option<String>> {
        let rows = self.conn.query(sql, &[])?.into_rows();
        Ok(rows.first().and_then(|row| row.first().cloned().flatten()))
    }

    /// Every row of `sql`, each rendered as `cell|cell|…`.
    ///
    /// Tests assert on whole tables rather than on one scalar, because the
    /// failure they prevent — a row algebra that deletes one row too many — is
    /// invisible when only a count is checked.
    pub(crate) fn rows(&mut self, sql: &str) -> Result<Vec<String>> {
        Ok(flattened(self.conn.query(sql, &[])?.into_rows()))
    }
}

/// Render query results as one `cell|cell|…` string per row, `NULL` for null.
#[cfg(any(test, feature = "testing"))]
pub(crate) fn flattened(rows: Vec<Vec<Option<String>>>) -> Vec<String> {
    rows.into_iter()
        .map(|row| {
            row.into_iter()
                .map(|cell| cell.unwrap_or_else(|| "NULL".to_owned()))
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The end-to-end obligation, against a real DuckLake catalog.
    ///
    /// It is skipped unless both `DUCKLAKE_DUCKDB_LIB` and
    /// `DUCKLAKE_EXTENSION_DIR` are set, because the connector deliberately does
    /// not build DuckDB in.  When it does run, it discharges the two claims the
    /// rest of the module rests on: the payload and the guard row are committed
    /// together, and a replayed step is a no-op rather than a second insert.
    #[test]
    fn a_published_step_and_a_replayed_step_leave_one_copy_of_its_rows() {
        use std::collections::BTreeMap;

        use feldera_types::transport::ducklake::{
            DEFAULT_ALIAS, DEFAULT_GUARD_TABLE, DEFAULT_SCHEMA, DuckLakeCatalogConfig,
            DuckLakeSinkConfig, DuckLakeWriterConfig,
        };
        use serde_json::json;

        let (Ok(_lib), Ok(ext_)) = (
            std::env::var("DUCKLAKE_DUCKDB_LIB"),
            std::env::var("DUCKLAKE_EXTENSION_DIR"),
        ) else {
            eprintln!(
                "skipping: set DUCKLAKE_DUCKDB_LIB and DUCKLAKE_EXTENSION_DIR to run against a \
                 real catalog"
            );
            return;
        };

        let dir = std::env::temp_dir().join(format!("ducklake-stage-b-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create temp dir");
        let meta = dir.join("meta.ducklake");
        let config = DuckLakeWriterConfig {
            catalog: DuckLakeCatalogConfig {
                catalog: format!("ducklake:{}", meta.display()),
                schema: DEFAULT_SCHEMA.to_owned(),
                alias: DEFAULT_ALIAS.to_owned(),
                data_path: None,
                extension_directory: Some(ext_),
                inlining_row_limit: None,
            },
            output_group: "g".to_owned(),
            generation: "gen".to_owned(),
            guard_table: DEFAULT_GUARD_TABLE.to_owned(),
            tables: BTreeMap::from([(
                "v".to_owned(),
                DuckLakeSinkConfig {
                    table: "t".to_owned(),
                    key_columns: vec![],
                },
            )]),
            epoch: None,
            send_snapshot: true,
        };

        let mut catalog = Catalog::open(&config).expect("open catalog");
        catalog
            .execute_sql(&format!(
                "CREATE TABLE {}.{}.t (id INTEGER, x VARCHAR)",
                DEFAULT_ALIAS, DEFAULT_SCHEMA
            ))
            .expect("create table");

        let batch = Batch {
            output_group: "g".to_owned(),
            generation: "gen".to_owned(),
            epoch: catalog.epoch(),
            base: 0,
            end: 7,
            deltas: vec![Delta::new("t", Op::Insert, json!({"id": 1, "x": "a"}))],
            resync: false,
        };

        let first = catalog.publish(&batch, 8).expect("publish");
        assert_eq!(first.decision, Decision::Apply);
        assert_eq!(first.guard.frontier, 7);
        assert_eq!(first.guard.batch_id.as_deref(), Some("gen:0->7"));

        // The replay the engine produces when a step is repeated.
        let replay = catalog.publish(&batch, 8).expect("replay");
        assert_eq!(replay.decision, Decision::Duplicate);

        let count = catalog
            .scalar(&format!(
                "SELECT count(*) FROM {DEFAULT_ALIAS}.{DEFAULT_SCHEMA}.t"
            ))
            .expect("count")
            .expect("count row");
        assert_eq!(count, "1", "the replayed step must not insert a second row");

        // A step whose interval has been overtaken is refused, not applied.
        let mut overtaken = batch.clone();
        overtaken.base = 5;
        overtaken.end = 6;
        overtaken.deltas = vec![Delta::new("t", Op::Insert, json!({"id": 2, "x": "b"}))];
        let stale = catalog.publish(&overtaken, 8).expect("stale publish");
        assert_eq!(stale.decision, Decision::Stale);
        assert_eq!(
            catalog
                .scalar(&format!(
                    "SELECT count(*) FROM {DEFAULT_ALIAS}.{DEFAULT_SCHEMA}.t"
                ))
                .expect("count")
                .expect("count row"),
            "1",
            "a stale step must not add rows"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn identifiers_that_are_not_plain_names_are_refused() {
        assert_eq!(quote_ident("orders").unwrap(), "\"orders\"");
        assert!(quote_ident("orders; DROP TABLE x").is_err());
        assert!(quote_ident("9lives").is_err());
        assert!(quote_ident("").is_err());
    }

    #[test]
    fn a_catalog_target_with_a_quote_cannot_break_out_of_its_literal() {
        assert_eq!(
            sql_literal("ducklake:/tmp/a'b.ducklake").unwrap(),
            "'ducklake:/tmp/a''b.ducklake'"
        );
    }

    #[test]
    fn transaction_conflicts_are_recognised_from_the_message() {
        // These are the shapes ducklake/duckdb produce when a commit loses a
        // write-write race; a retry is correct only for these.
        assert!(is_transaction_conflict(
            "TransactionContext Error: Failed to commit: conflict on table"
        ));
        assert!(is_transaction_conflict("optimistic write conflict"));
        assert!(!is_transaction_conflict(
            "Catalog Error: Table with name x does not exist"
        ));
    }
}
