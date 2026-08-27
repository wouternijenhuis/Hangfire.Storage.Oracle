# DevDad.Hangfire.Oracle

[![CI](https://github.com/wouternijenhuis/Hangfire.Storage.Oracle/actions/workflows/ci.yml/badge.svg)](https://github.com/wouternijenhuis/Hangfire.Storage.Oracle/actions/workflows/ci.yml)
[![NuGet](https://img.shields.io/nuget/v/DevDad.Hangfire.Oracle.svg)](https://www.nuget.org/packages/DevDad.Hangfire.Oracle)
[![License](https://img.shields.io/github/license/wouternijenhuis/Hangfire.Storage.Oracle)](LICENSE)

Oracle Database storage for Hangfire. Version 1.0.4 is a backward-compatible stabilization release for .NET 8 and .NET 10. It preserves the `DevDad.Hangfire.Oracle` package ID and the `Hangfire.Oracle.Core` namespace and assembly.

## Requirements

- .NET 8.0 or .NET 10.0
- Oracle Database 19c or later
- A database user that can create tables, sequences, and indexes when automatic schema preparation is enabled
- `EXECUTE` on `DBMS_LOCK` only when `UseDbmsLock` is enabled

## Install

```xml
<PackageReference Include="DevDad.Hangfire.Oracle" Version="1.0.4" />
```

```powershell
dotnet add package DevDad.Hangfire.Oracle --version 1.0.4
```

## Configure

```csharp
using Hangfire;
using Hangfire.Oracle.Core;

builder.Services.AddHangfire(configuration => configuration
    .SetDataCompatibilityLevel(CompatibilityLevel.Version_180)
    .UseSimpleAssemblyNameTypeSerializer()
    .UseRecommendedSerializerSettings()
    .UseOracleStorage(
        builder.Configuration.GetConnectionString("HangfireOracle")!,
        new OracleStorageOptions
        {
            PrepareSchemaIfNecessary = true,
            SchemaName = "HANGFIRE",
            TablePrefix = "HF_"
        }));

builder.Services.AddHangfireServer();
```

For non-ASP.NET Core applications:

```csharp
using Hangfire;
using Hangfire.Oracle.Core;

GlobalConfiguration.Configuration.UseOracleStorage(
    "User Id=hangfire;Password=secret;Data Source=localhost/FREEPDB1");
```

## Schema installation and upgrades

`PrepareSchemaIfNecessary` defaults to `true`. Startup installs the schema idempotently and records its schema version. A complete 1.0.3 schema is migrated in place to schema version 2; job and Hangfire data are retained.

Stop all Hangfire workers before the first 1.0.4 startup. Back up the Oracle schema, deploy one 1.0.4 instance with schema preparation enabled, verify startup, and then start the remaining workers. See [UPGRADE.md](UPGRADE.md) for the checklist.

For database-managed deployments, the embedded scripts are also packaged under `contentFiles/any/any/Sql`. Run `Install.sql` with the desired schema and prefix substitutions. Automatic preparation remains the recommended path because it applies versioned migrations.

Oracle identifiers are validated as unquoted identifiers. `SchemaName` and `TablePrefix` may contain letters, digits, `_`, `$`, and `#`; they must begin with a letter and produce identifiers no longer than 128 characters.

## Options

| Option | Default | Purpose |
| --- | ---: | --- |
| `SchemaName` | current user | Owner of the Hangfire objects |
| `TablePrefix` | `HF_` | Prefix for every table, sequence, and index |
| `PrepareSchemaIfNecessary` | `true` | Install or migrate the schema during storage construction |
| `MinimumDatabaseVersion` | `Oracle19c` | Supported SQL compatibility floor; 1.0.4 rejects older values |
| `UseSkipLocked` | `true` | Use concurrent `FOR UPDATE SKIP LOCKED` queue acquisition |
| `InvisibilityTimeout` | 30 minutes | Time after which an abandoned fetch is made visible |
| `SlidingInvisibilityTimeout` | 5 minutes | Lease-refresh interval basis for a running fetched job |
| `QueuePollInterval` | 15 seconds | Delay after an empty queue poll |
| `FetchCount` | `1` | Maximum immediate acquisition attempts before the poll delay |
| `TransactionIsolationLevel` | `ReadCommitted` | Isolation used by write transactions |
| `TransactionTimeout` | 1 minute | Upper bound used for transaction-scoped commands |
| `CommandTimeout` | 30 seconds | Oracle command timeout; `0` means unlimited |
| `MaxRetryAttempts` | `3` | Additional retries for classified transient Oracle failures |
| `RetryDelay` | 100 ms | Initial exponential retry delay |
| `DistributedLockTimeout` | 10 minutes | Default lock wait/lease duration |
| `UseDbmsLock` | `false` | Select `DBMS_LOCK` instead of owner-aware table leases |
| `JobExpirationCheckInterval` | 30 minutes | Expired-record cleanup interval |
| `CounterAggregationInterval` | 5 minutes | Raw-counter aggregation interval |
| `CleanupBatchSize` | `1000` | Maximum cleanup/aggregation batch size |
| `DashboardJobListLimit` | `50000` | Maximum number of rows exposed to dashboard lists |
| `UseUtcTime` | `true` | Use UTC rather than server-local timestamps |
| `EnableStatementCaching` | `true` | Add ODP.NET statement-cache settings when absent |
| `StatementCacheSize` | `50` | Per-connection ODP.NET statement cache size |

Invalid identifiers, non-positive durations and sizes, negative timeout/retry values, and unsupported database versions fail during storage construction.

## Dependencies

The 1.0.4 package directly references:

| Package | Version |
| --- | --- |
| Hangfire.Core | 1.8.24 |
| Oracle.ManagedDataAccess.Core | 23.26.300 |
| Dapper | 2.1.79 |
| Newtonsoft.Json | 13.0.3 (pinned transitive Hangfire dependency) |

`Dapper.Oracle` is no longer used or included.

## Development

The repository uses the .NET SDK selected by `global.json`, locked NuGet restores, xUnit v3, Microsoft Testing Platform, Oracle Database Free 23 integration tests, package compatibility validation against 1.0.3, and enforced coverage thresholds. See [CONTRIBUTING.md](CONTRIBUTING.md) and [RELEASE.md](RELEASE.md).

## Support and security

Use [GitHub Issues](https://github.com/wouternijenhuis/Hangfire.Storage.Oracle/issues) for reproducible defects and feature requests. Report vulnerabilities according to [SECURITY.md](SECURITY.md).

Licensed under the [MIT License](LICENSE).
