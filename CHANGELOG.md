# Changelog

All notable changes are documented here. The project follows Semantic Versioning.

## [1.0.4] - 2026-08-27

### Fixed

- Replaced fragile SQL splitting and partial-object detection with versioned, idempotent schema installation and an in-place 1.0.3 migration.
- Corrected schema/prefix qualification, identifier validation, sequence names, and unexpected DDL error handling.
- Made Oracle queue acquisition use valid `FOR UPDATE SKIP LOCKED` semantics through one implementation while retaining the public queue type.
- Added fetch ownership tokens, sliding visibility renewal, abandoned-fetch recovery, and owner-safe remove/requeue behavior.
- Made counter aggregation lock, aggregate, and delete the same batch atomically, and included raw counters in reads and statistics.
- Made table locks owner-aware with lease renewal and stale-owner protection; implemented the existing `DBMS_LOCK` option.
- Corrected monitoring DTOs, state-name handling, pagination, list limits, time handling, cleanup targets, command timeouts, and rollback paths.

### Changed

- Updated Hangfire.Core to 1.8.24, Oracle.ManagedDataAccess.Core to 23.26.300, and Dapper to 2.1.79.
- Removed the unused deprecated Dapper.Oracle dependency.
- Migrated tests to xUnit v3 4.0.0 and Microsoft Testing Platform.
- Added .NET 8 and .NET 10 Oracle integration coverage using Oracle Database Free 23.
- Added deterministic builds, Source Link, symbols, XML documentation, package compatibility validation, locked restores, dependency auditing, package inspection, and install smoke tests.

### Compatibility

- Package ID remains `DevDad.Hangfire.Oracle`.
- Namespace and assembly remain `Hangfire.Oracle.Core`.
- Target frameworks remain `net8.0` and `net10.0`.
- Public API compatibility is validated against package 1.0.3.

[1.0.4]: https://github.com/wouternijenhuis/Hangfire.Storage.Oracle/releases/tag/v1.0.4
