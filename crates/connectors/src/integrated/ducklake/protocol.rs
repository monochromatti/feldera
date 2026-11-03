//! The publication protocol: batch identity, the guard row, and the decision.
//!
//! This is the native counterpart of the Python bridge's `protocol.py`, and the
//! vocabulary is deliberately the same so that a catalog written by one is read
//! by the other:
//!
//! `frontier`
//!     The batch id the group has committed.  `0` means "nothing yet".  Here it
//!     is a DBSP **transaction id**, which the engine reproduces deterministically
//!     across replay, so it is stable across restarts of the process.
//!
//! `generation`
//!     Identifies the pipeline and SQL program.  A batch from another generation
//!     is refused outright: applying it would mix two runs' results, and unlike
//!     the epoch there is no safe automatic resolution.
//!
//! `epoch`
//!     Ownership of the group.  Taking over means committing a strictly larger
//!     epoch; a writer holding a stale epoch is then fenced by the guard row
//!     rather than by a lock.
//!
//! `resync`
//!     A batch that replaces the group's whole content with a snapshot.
//!
//! The decision function is total: every (guard, batch) pair maps to exactly
//! one of apply / duplicate / stale / wrong generation / wrong owner.  Two
//! properties are the ones the design leans on, and both are asserted in the
//! tests below rather than left to inspection:
//!
//! * a batch that was applied is a duplicate when replayed (idempotence), and
//! * a batch whose interval has been overtaken is stale, never applied on top
//!   (fencing).
//!
//! Where this differs from `protocol.py`, and why: there the frontier was a step
//! counter that restarted with the bridge process, so a replay could only be
//! recognised by the batch identity `generation:base->end`.  A DBSP transaction
//! id is monotone across replay, so `frontier >= end` decides the question
//! without consulting the identity — and `base` degrades from identity input to
//! a consistency assertion, which is why a mismatch is stale rather than a
//! rewrite of history.

use anyhow::{Result, bail};
use serde_json::Value;
use sha2::{Digest as _, Sha256};

/// Frontier of a group that has not published anything.
pub const NOWHERE: u64 = 0;

/// Feldera's insert/delete update format.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Op {
    Insert,
    Delete,
}

impl Op {
    pub fn as_str(self) -> &'static str {
        match self {
            Op::Insert => "insert",
            Op::Delete => "delete",
        }
    }
}

/// One row event, in Feldera's insert/delete representation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Delta {
    pub table: String,
    pub op: Op,
    pub row: Value,
}

impl Delta {
    pub fn new(table: impl Into<String>, op: Op, row: Value) -> Delta {
        Delta {
            table: table.into(),
            op,
            row,
        }
    }

    /// Stable rendering used for ordering and for the digest.
    ///
    /// Object keys are sorted explicitly rather than relying on `serde_json`'s
    /// map type: with the `preserve_order` feature enabled the map follows
    /// insertion order, and then two rows with the same content but a different
    /// key order would hash differently and a replay would look like a payload
    /// change.
    pub fn canonical(&self) -> String {
        format!(
            "{}:{}:{}",
            self.table,
            self.op.as_str(),
            canonical_json(&self.row)
        )
    }
}

fn canonical_json(value: &Value) -> String {
    match value {
        Value::Object(map) => {
            let mut entries: Vec<(&String, &Value)> = map.iter().collect();
            entries.sort_by(|a, b| a.0.cmp(b.0));
            let inner: Vec<String> = entries
                .into_iter()
                .map(|(k, v)| {
                    format!(
                        "{}:{}",
                        serde_json::to_string(k).unwrap_or_else(|_| "\"\"".to_owned()),
                        canonical_json(v)
                    )
                })
                .collect();
            format!("{{{}}}", inner.join(","))
        }
        Value::Array(items) => {
            let inner: Vec<String> = items.iter().map(canonical_json).collect();
            format!("[{}]", inner.join(","))
        }
        other => serde_json::to_string(other).unwrap_or_else(|_| "null".to_owned()),
    }
}

/// A unit of publication output, tagged with its identity.
///
/// The claim it makes: "when `frontier` is `base`, the group's new state is the
/// old state plus these deltas, and the group's frontier becomes `end`".
#[derive(Debug, Clone)]
pub struct Batch {
    pub output_group: String,
    pub generation: String,
    pub epoch: u64,
    pub base: u64,
    pub end: u64,
    pub deltas: Vec<Delta>,
    pub resync: bool,
}

impl Batch {
    /// Identity of the batch: everything except who submits it and when.
    pub fn batch_id(&self) -> String {
        format!("{}:{}->{}", self.generation, self.base, self.end)
    }

    /// Digest over the payload, order-insensitively.
    ///
    /// Deltas are sorted first: Feldera's output chunk order is not meaningful,
    /// so treating it as meaningful would make a replay look like new content.
    pub fn digest(&self) -> String {
        let mut parts: Vec<String> = self.deltas.iter().map(Delta::canonical).collect();
        parts.sort();
        if self.resync {
            parts.push("resync".to_owned());
        }
        let mut hasher = Sha256::new();
        hasher.update(parts.join("\n").as_bytes());
        hex::encode(hasher.finalize())
    }
}

/// The protected singleton row of an output group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Guard {
    pub output_group: String,
    pub generation: String,
    pub epoch: u64,
    pub frontier: u64,
    pub batch_id: Option<String>,
    /// Digest of the batch that set `frontier`.
    ///
    /// Not part of the identity: the identity is the generation and the
    /// interval, because a replayed step may legitimately arrive with the same
    /// identity.  It is a divergence detector — the same identity with
    /// different content is a real anomaly, and the connector stops rather than
    /// guessing which version is true.
    pub digest: Option<String>,
}

impl Guard {
    pub fn initial(output_group: impl Into<String>, generation: impl Into<String>) -> Guard {
        Guard {
            output_group: output_group.into(),
            generation: generation.into(),
            epoch: 1,
            frontier: NOWHERE,
            batch_id: None,
            digest: None,
        }
    }
}

/// What a publisher should do with a batch, given a guard.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    Apply,
    Duplicate,
    Stale,
    WrongGeneration,
    WrongOwner,
}

impl Decision {
    pub fn as_str(self) -> &'static str {
        match self {
            Decision::Apply => "apply",
            Decision::Duplicate => "duplicate",
            Decision::Stale => "stale",
            Decision::WrongGeneration => "wrong_generation",
            Decision::WrongOwner => "wrong_owner",
        }
    }
}

/// The protocol's total decision function.
///
/// Order matters.  Generation first, because a batch from another run shares no
/// interval with this one and nothing about it is interpretable.  Then
/// ownership, because a fenced writer must not be able to reason about the
/// frontier at all.  Then the frontier comparison, which is what makes a replay
/// a duplicate and an overtaken interval stale.  `base` is checked last: it can
/// only detect an inconsistency in the caller, since `frontier` already decides.
pub fn decide(guard: &Guard, batch: &Batch) -> Result<Decision> {
    if batch.generation != guard.generation {
        return Ok(Decision::WrongGeneration);
    }
    if batch.epoch != guard.epoch {
        return Ok(Decision::WrongOwner);
    }
    if guard.frontier > batch.end {
        // A newer transaction is already published.  The engine can replay an
        // older step; applying it would move the group's content backwards.
        return Ok(Decision::Stale);
    }
    if guard.frontier == batch.end {
        if let Some(committed) = &guard.digest {
            let received = batch.digest();
            if *committed != received {
                bail!(
                    "batch {} has the same identity as the committed batch of {:?} but different \
                     content (committed {}, received {}); refusing to treat a divergence as a \
                     duplicate",
                    batch.batch_id(),
                    guard.output_group,
                    &committed[..12.min(committed.len())],
                    &received[..12.min(received.len())],
                );
            }
        }
        return Ok(Decision::Duplicate);
    }
    if batch.resync {
        return Ok(Decision::Apply);
    }
    if guard.frontier != batch.base {
        return Ok(Decision::Stale);
    }
    Ok(Decision::Apply)
}

/// The guard state after applying `batch`.
pub fn advance(guard: &Guard, batch: &Batch) -> Guard {
    Guard {
        frontier: batch.end,
        batch_id: Some(batch.batch_id()),
        digest: Some(batch.digest()),
        ..guard.clone()
    }
}

/// Take ownership of an output group.
///
/// `epoch` must strictly exceed the current one.  Bumping is the only operation
/// that changes ownership and it never lowers the epoch, so a publisher that has
/// not yet observed the bump is fenced by the conditional guard update rather
/// than by a lock.
pub fn acquire_ownership(guard: &Guard, epoch: u64) -> Result<Guard> {
    if epoch <= guard.epoch {
        bail!(
            "epoch {epoch} does not acquire ownership of {:?} (current epoch {})",
            guard.output_group,
            guard.epoch
        );
    }
    Ok(Guard {
        epoch,
        ..guard.clone()
    })
}

/// Reject batches that are meaningless before they reach the catalog.
pub fn validate(batch: &Batch, known_tables: &[String]) -> Result<()> {
    if batch.output_group.is_empty() {
        bail!("batch has no output group");
    }
    if batch.generation.is_empty() {
        bail!("batch has no generation");
    }
    if batch.epoch < 1 {
        bail!("batch epoch {} is not a positive integer", batch.epoch);
    }
    if batch.end < batch.base {
        bail!("batch {} has end before base", batch.batch_id());
    }
    for delta in &batch.deltas {
        if delta.table.is_empty() {
            bail!("delta has no table");
        }
        if !matches!(delta.row, Value::Object(_)) {
            bail!(
                "delta on {:?} is not a JSON object but {}",
                delta.table,
                delta.row
            );
        }
        if !known_tables.iter().any(|t| *t == delta.table) {
            bail!(
                "batch writes table {:?}, which this output group does not own (it owns {known_tables:?})",
                delta.table
            );
        }
    }
    if batch.resync && batch.base == batch.end {
        bail!("resync batch {} makes no progress", batch.batch_id());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    //! The obligations these tests discharge, phrased as the failure they
    //! prevent: a published step being applied twice (double-counted SUM), an
    //! overtaken step being applied on top of newer content (silent
    //! corruption), and a fenced writer publishing anyway.

    use serde_json::json;

    use super::*;

    fn batch(generation: &str, epoch: u64, base: u64, end: u64) -> Batch {
        Batch {
            output_group: "g".to_owned(),
            generation: generation.to_owned(),
            epoch,
            base,
            end,
            deltas: vec![Delta::new("t", Op::Insert, json!({"id": end}))],
            resync: false,
        }
    }

    fn guard(frontier: u64) -> Guard {
        Guard {
            frontier,
            batch_id: Some(format!("gen:0->{frontier}")),
            digest: None,
            ..Guard::initial("g", "gen")
        }
    }

    fn decided(guard: &Guard, batch: &Batch) -> Decision {
        decide(guard, batch).expect("decide must not fail")
    }

    #[test]
    fn a_batch_on_top_of_the_frontier_applies() {
        assert_eq!(decided(&guard(3), &batch("gen", 1, 3, 4)), Decision::Apply);
    }

    #[test]
    fn replaying_the_committed_batch_is_a_duplicate_not_a_second_apply() {
        // The engine replays a step when the endpoint acknowledged but the
        // process died before the guard was observed; re-applying would
        // double-count the step's rows.
        assert_eq!(
            decided(&guard(7), &batch("gen", 1, 6, 7)),
            Decision::Duplicate
        );
    }

    #[test]
    fn an_overtaken_interval_is_stale_even_with_an_older_base() {
        assert_eq!(decided(&guard(9), &batch("gen", 1, 7, 8)), Decision::Stale);
    }

    #[test]
    fn a_batch_whose_base_is_not_the_frontier_is_stale() {
        // Two publishers saw the same frontier; one won.  The other's base is
        // no longer the frontier even though its interval is ahead.
        assert_eq!(decided(&guard(5), &batch("gen", 1, 4, 6)), Decision::Stale);
    }

    #[test]
    fn a_resync_applies_from_an_unrelated_frontier() {
        // A snapshot replaces the group's content, so its base is not an
        // interval boundary and must not be compared to the frontier.
        let mut resync = batch("gen", 1, 0, 12);
        resync.resync = true;
        assert_eq!(decided(&guard(5), &resync), Decision::Apply);
    }

    #[test]
    fn another_generation_is_refused_outright() {
        // Mixing two runs' results is not something the guard can resolve, so it
        // is an error rather than a retry.
        assert_eq!(
            decided(&guard(3), &batch("other", 1, 3, 4)),
            Decision::WrongGeneration
        );
    }

    #[test]
    fn a_fenced_writer_is_refused_even_with_a_higher_epoch() {
        // Ownership is all-or-nothing: a batch claiming a higher epoch than the
        // guard holds has not been granted it, so it is fenced.
        assert_eq!(
            decided(&guard(3), &batch("gen", 4, 3, 4)),
            Decision::WrongOwner
        );
    }

    #[test]
    fn the_same_identity_with_different_content_stops_the_writer() {
        let committed = batch("gen", 1, 6, 7);
        let mut g = guard(7);
        g.digest = Some(committed.digest());
        let mut diverged = batch("gen", 1, 6, 7);
        diverged.deltas = vec![Delta::new("t", Op::Insert, json!({"id": 999}))];
        let err = decide(&g, &diverged).expect_err("a divergence must not be a duplicate");
        assert!(
            format!("{err}").contains("different content"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn advancing_records_the_identity_and_the_digest() {
        let b = batch("gen", 1, 3, 4);
        let g = advance(&guard(3), &b);
        assert_eq!(g.frontier, 4);
        assert_eq!(g.batch_id.as_deref(), Some("gen:3->4"));
        assert_eq!(g.digest, Some(b.digest()));
    }

    #[test]
    fn the_digest_ignores_delta_order() {
        let a = Delta::new("t", Op::Insert, json!({"id": 1, "x": 2}));
        let b = Delta::new("t", Op::Insert, json!({"x": 2, "id": 1}));
        let mut first = batch("gen", 1, 0, 1);
        let mut second = batch("gen", 1, 0, 1);
        first.deltas = vec![a.clone(), b.clone()];
        second.deltas = vec![b, a];
        assert_eq!(first.digest(), second.digest());
    }

    #[test]
    fn taking_over_requires_a_strictly_greater_epoch() {
        let g = guard(0);
        assert!(acquire_ownership(&g, 1).is_err());
        assert_eq!(acquire_ownership(&g, 2).unwrap().epoch, 2);
    }

    #[test]
    fn a_batch_for_an_unowned_table_is_rejected_before_it_reaches_the_catalog() {
        let b = batch("gen", 1, 0, 1);
        let err = validate(&b, &["other".to_owned()]).expect_err("table is not owned");
        assert!(format!("{err}").contains("does not own"), "{err}");
    }
}
