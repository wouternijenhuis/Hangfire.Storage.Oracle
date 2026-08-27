# Upgrade to 1.0.4

Version 1.0.4 automatically upgrades a complete 1.0.3 schema without deleting Hangfire data. The migration adds ownership and lease columns to the distributed-lock table and records schema version 2.

## Before deployment

1. Confirm the production database is Oracle 19c or later.
2. Stop every Hangfire worker and application instance that uses this schema.
3. Back up the Hangfire schema and retain a tested restore procedure.
4. Confirm the runtime user can alter the existing Hangfire tables and create indexes.
5. Preserve the existing `SchemaName` and `TablePrefix` values.

## Deploy

1. Update `DevDad.Hangfire.Oracle` to 1.0.4.
2. Start one application instance with `PrepareSchemaIfNecessary = true`.
3. Verify that startup succeeds and the schema-version hash contains version `2`.
4. Verify queued, scheduled, succeeded, failed, and recurring jobs in the dashboard.
5. Start the remaining workers.

Schema installation is idempotent, so later 1.0.4 startups validate rather than recreate complete objects. Unexpected Oracle DDL errors are surfaced and must be investigated.

## Rollback

The added lock columns are compatible with stored 1.0.3 data, but application rollback should use the database backup taken before migration. Stop workers before restoring. Do not run `Uninstall.sql` as a rollback mechanism; it removes Hangfire data.

## Behavior to review

- Oracle versions older than 19c are rejected.
- Identifiers are validated and normalized as unquoted Oracle identifiers.
- A fetched job can only be removed or requeued by its current fetch owner.
- Table-backed locks use owner-aware renewable leases. `UseDbmsLock = true` requires the Oracle `DBMS_LOCK` privilege.
- Counter and dashboard totals now include both pending raw counters and aggregated counters.
