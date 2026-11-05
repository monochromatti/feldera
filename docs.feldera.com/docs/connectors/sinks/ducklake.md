# DuckLake output connector

[DuckLake](https://ducklake.select/) is an open lakehouse format: a SQL catalog holds table
metadata and a transaction history, and the table data lives as Parquet files next to it. A
DuckLake table is therefore updatable and deletable in place, which a plain set of Parquet
files is not.

The DuckLake output connector writes a view's changes into a DuckLake table — inserts,
deletes and updates — inside the catalog's own transaction. It is an *integrated* connector:
the transport is a DuckDB connection and the format is DuckLake's own, so a connector
configuration has no `format` section.

The connector supports [fault tolerance](/pipelines/fault-tolerance): a pipeline that
restarts from a checkpoint neither loses an output step nor writes one twice. How that works
is described in [publication](#publication-and-fault-tolerance) below.

## Overview

```sql
-- The table is created by the catalog owner, not by the connector.
-- In the catalog: CREATE TABLE main.orders (id BIGINT, amount DECIMAL(12,2), ...);

CREATE VIEW orders_out
WITH (
    'connectors' = '[{
        "transport": {
            "name": "ducklake_output",
            "config": {
                "catalog": "ducklake:postgres:dbname=ducklake host=db.internal",
                "table": "orders",
                "output_group": "orders",
                "generation": "v1"
            }
        }
    }]'
)
AS SELECT id, amount FROM orders;
```

## Publication and fault tolerance

The engine and the catalog cannot commit together, so the connector keeps a **guard row** per
output group in the catalog, and updates it in the same transaction as the rows it describes:

| Column         | Type      | Meaning                                                            |
|----------------|-----------|--------------------------------------------------------------------|
| `output_group` | `VARCHAR` | Name of the group (unique)                                         |
| `generation`   | `VARCHAR` | Identity of this publication arrangement                            |
| `epoch`        | `BIGINT`  | Ownership epoch, incremented to take a group over                   |
| `frontier`     | `BIGINT`  | Last step whose output is in the catalog                            |
| `batch_id`     | `VARCHAR` | Identity of that step                                              |
| `digest`       | `VARCHAR` | Fingerprint of the rows that step published                         |
| `updated_at`   | `TIMESTAMP` | When the row was last written                                     |

A step publishes in one transaction: delete and insert the step's rows, then advance the
guard row. A step the guard row already covers is a no-op — and if the guard row names that
step but records a different digest, the connector stops instead of guessing, because the
catalog then holds output that the engine did not produce.

The connector does not declare a fault-tolerance model of its own — that is the pipeline's
setting. What it does do is keep exactly-once pipelines exactly-once: a pipeline with
`"fault_tolerance": {"model": "exactly_once"}` that restarts from a checkpoint neither loses
a step's output nor publishes one twice. The guard row is what makes a step idempotent; the
catalog is the only place the connector records progress.

## DuckLake output connector configuration

| Property                 | Type    | Default            | Description |
|--------------------------|---------|--------------------|-------------|
| `catalog`*               | string  |                    | Catalog to attach, in DuckDB's `ATTACH` grammar: `ducklake:/path/to/meta.ducklake`, `ducklake:postgres:dbname=ducklake host=...`. The connector passes it through unchanged. |
| `table`*                 | string  |                    | DuckLake table to write the view's rows to. Must already exist; see [schema contract](#schema-contract). |
| `output_group`*          | string  |                    | Name of the publication group. One guard row per group: two connectors sharing a group fence each other off instead of interleaving writes to the same rows. |
| `generation`*            | string  |                    | Identity of this publication arrangement, for example a pipeline generation. Part of every batch identity. |
| `guard_table`            | string  | `connector_guard`  | Table holding the group's guard row, in the catalog schema below. |
| `schema`                 | string  | `main`             | DuckLake schema that holds the output tables and the guard table. |
| `key_columns`            | array   | `[]`               | Columns that identify a row. With key columns the connector replaces by key; without them it deletes one matching row and inserts one, keeping the table a bag. |
| `alias`                  | string  | `dl`               | Name under which the catalog is attached on the connector's connection. It must differ from the name of any other catalog the pipeline attaches. |
| `epoch`                  | integer |                    | Ownership epoch. Must strictly increase to take over a group from a previous owner; see [taking over a group](#taking-over-a-group). |
| `send_snapshot`          | bool    | `true`             | Resynchronize from the engine's snapshot when the guard row is behind the tables. Disable to turn that situation into a startup error instead. |
| `data_path`              | string  |                    | Directory for DuckLake's data files, overriding the catalog's own setting. Needed when the catalog was created with a data path that is not valid from the pipeline host. |
| `extension_directory`    | string  |                    | Directory holding `ducklake.duckdb_extension`. Unset lets DuckDB download it on first use, which fails on a host without egress; see [deployment](#deployment). Must contain, or be writable enough for DuckDB to install, every extension the catalog needs — a PostgreSQL-backed catalog also needs `postgres_scanner`. |
| `inlining_row_limit`     | integer | catalog's default  | Rows per data file stored inline in the catalog. |

[*]: Required fields

## Schema contract

The connector writes into a table it does not create. What it expects of that table:

- **The columns exist, by name.** The view's column names are the table's column names.
  There is no mapping option and no positional matching.
- **The types are assignable.** Values are bound as query parameters, not interpolated, so
  DuckDB's own coercion applies: a `BIGINT` view column fits a `DECIMAL(12,2)` table column,
  and a mismatch that DuckDB refuses is a write error rather than a silent truncation. Nested
  values (`STRUCT`, `LIST`, `MAP`) are written as the JSON the engine emits and cast by
  DuckDB to the column's type.
- **Deletes can be expressed.** A delete is a `DELETE` matching the column values with `IS
  NOT DISTINCT FROM`, limited to one row — so a table with no key columns must tolerate
  duplicate rows being distinct objects, which is exactly bag semantics. Declaring
  `key_columns` makes the delete a key match instead, which is what a table with a primary
  key wants.
- **The connector's own columns do not collide.** `connector_guard` in the catalog schema is
  the connector's; a table of that name is not usable as an output table.

## Taking over a group

The guard row belongs to the group, not to a pipeline. A new pipeline that inherits the row
must either keep the `generation` that pinned it, or take the group over with a strictly
greater `epoch`. A changed generation under an unchanged epoch is refused: the connector
cannot tell a rename from a mistake, and guessing would publish two histories into one table.

## Limitations

- **The file-backed catalog is single-process.** DuckLake's metadata is a DuckDB database
  file, and DuckDB's file locks are exclusive, so a second process cannot even attach the
  catalog while the first holds it. A reader and a writer can therefore coexist in one
  pipeline but not in two, unless the catalog is PostgreSQL-backed. A PostgreSQL catalog
  does support concurrent publishers: a write-write conflict is detected at `COMMIT`, and a
  second publisher of the same step finds the guard row advanced and does nothing.
- **`enable_output_buffer` is refused** for the output: a buffered batch covers several steps,
  and the guard row can name only one, so the connector rejects the configuration at startup
  rather than publishing output it cannot account for.
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
