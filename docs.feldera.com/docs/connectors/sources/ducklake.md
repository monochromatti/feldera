# DuckLake input connector

[DuckLake](https://ducklake.select/) is an open lakehouse format: a SQL catalog holds table
metadata and a transaction history, and the table data lives as Parquet files next to it.

The DuckLake input connector reads a DuckLake table into a Feldera table as a stream of
changes. It is an *integrated* connector: the transport is a DuckDB connection and the format
is DuckLake's own, so a connector configuration has no `format` section.

## Overview

```sql
CREATE TABLE orders (
    id BIGINT,
    amount DECIMAL(12,2)
) WITH (
    'connectors' = '[{
        "transport": {
            "name": "ducklake_input",
            "config": {
                "catalog": "ducklake:postgres:dbname=ducklake host=db.internal",
                "table": "orders"
            }
        }
    }]'
);
```

## What is read

The connector reads the table's **current contents** first, then **follows the catalog's
snapshot history** for every change committed after it. Both come from the same DuckLake
function over a snapshot range, so "bootstrap" and "incremental" are one query shape with
different bounds rather than two code paths that can disagree.

An update arrives as the deleted row and the inserted row, in that order, which is what the
rest of Feldera expects from a change stream. The connector only ever emits row-level
changes: a statement in the catalog that rewrites many rows, `TRUNCATE` included, arrives as
the deletes and inserts that the snapshot recorded, not as a table replacement.

The connector polls: DuckLake exposes changes as a query over a snapshot range, not as a
subscription. `poll_interval` is the floor on end-to-end latency, not a rate limit — a poll
that finds nothing costs one query against the catalog.

## Fault tolerance

The connector supports [fault tolerance](/pipelines/fault-tolerance) with **at-least-once**
delivery. The snapshot the connector has read up to is part of the checkpoint, so a restart
resumes from that snapshot rather than from the table's beginning. The declared mode is
at-least-once rather than exactly-once because the catalog read and the engine's checkpoint
are not one transaction: a step that was read but whose checkpoint did not commit is read
again.

## DuckLake input connector configuration

| Property                | Type    | Default           | Description |
|-------------------------|---------|-------------------|-------------|
| `catalog`*              | string  |                   | Catalog to attach, in DuckDB's `ATTACH` grammar: `ducklake:/path/to/meta.ducklake`, `ducklake:postgres:dbname=ducklake host=...`. The connector passes it through unchanged, attached read-only. |
| `table`*                | string  |                   | DuckLake table to read. Must be a table; see [limitations](#limitations). |
| `poll_interval`         | duration| `1s`              | How often to ask the catalog for new changes, for example `500ms`. |
| `alias`                 | string  | `dl`              | Name under which the catalog is attached on the connector's connection. It must differ from the name of any other catalog the pipeline attaches. |
| `data_path`             | string  |                   | Directory holding DuckLake's data files, overriding the catalog's own setting. Needed when the catalog was created with a data path that is not valid from the pipeline host. |
| `extension_directory`   | string  |                   | Directory holding `ducklake.duckdb_extension`. Unset lets DuckDB download it on first use, which fails on a host without egress; see [deployment](#deployment). Must contain, or be writable enough for DuckDB to install, every extension the catalog needs; a PostgreSQL-backed catalog also needs `postgres_scanner`. |
| `inlining_row_limit`    | integer | catalog's default | Rows per data file stored inline in the catalog. |

[*]: Required fields

## Schema contract

- **The columns are matched by name.** The Feldera table's column names are the DuckLake
  table's column names; there is no positional matching and no rename mapping.
- **The types must be castable.** Rows cross the boundary as JSON, so what the catalog stores
  must parse into the Feldera column's type: `DECIMAL` into `DECIMAL`, `TIMESTAMP` into
  `TIMESTAMP`, `STRUCT`/`LIST`/`MAP` into the same shape. A value that does not fit is a
  deserialization error, reported per row.
- **The table is created by the catalog owner.** The connector does not create it; a missing
  table is a connector error, not an empty stream.

## Limitations

- **Base tables only.** A DuckLake view has no snapshots of its own, so reading one yields no
  rows — the connector cannot bootstrap from it and does not follow its inputs. Read the
  table, and do the projection in SQL inside Feldera.
- **The file-backed catalog is single-process.** DuckLake's metadata is a DuckDB database
  file with exclusive file locks, so a second process cannot attach the catalog while the
  first holds it. A PostgreSQL-backed catalog allows a reader and a writer in different
  processes; that is the configuration to use when two pipelines share a catalog.
- **Changes are visible only after they commit.** An uncommitted transaction in another
  connection is not read, which is what makes the read repeatable, but it also means the
  connector has no way to see a writer's in-flight work.
- **The DuckDB library and the DuckLake extension are pinned together.** Extension binaries
  are platform-specific and version-locked: the connector requires the DuckDB 2.0 C library
  and a DuckLake extension built against it.

## Deployment

`libduckdb` and `ducklake.duckdb_extension` are not part of the Feldera distribution; the
pipeline image has to provide both. Vendor the extension rather than letting DuckDB download
it at run time — a host without egress would otherwise fail on the first attach, and the
download would make a production deployment depend on the extension registry being up.
`extension_directory` points at the vendored copy; the extension is signature-checked
against DuckDB's built-in keys, so the official build loads without weakening that check.

A PostgreSQL-backed catalog additionally needs `postgres_scanner`, which DuckDB loads when
attaching it — vendor that extension as well.
