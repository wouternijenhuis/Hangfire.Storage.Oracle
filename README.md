# DevDad.Hangfire.Oracle

A Hangfire storage provider implementation for Oracle Database using Oracle.ManagedDataAccess.Core and Dapper.

## Features

- **Full Hangfire Storage Implementation**: Implements all required Hangfire storage interfaces
  - `OracleStorage` : `JobStorage`
  - `IStorageConnection` for read operations
  - `IWriteOnlyTransaction` for write operations
  - `IMonitoringApi` for dashboard and monitoring

- **Oracle Database Support**: Uses Oracle.ManagedDataAccess.Core for reliable Oracle connectivity
- **Oracle 19c+ Optimizations**: Takes advantage of modern Oracle features:
  - `FOR UPDATE SKIP LOCKED` for non-blocking job queue polling
  - `FETCH FIRST N ROWS ONLY` for efficient pagination
  - MERGE statements for atomic upserts
  - Sequence caching for improved insert performance
- **High Performance**: Leverages Dapper async APIs with retry logic
- **Distributed Locks**: Ensures job processing coordination across multiple servers
- **Automatic Retry**: Transient database errors are automatically retried with exponential backoff
- **Connection Pooling**: Optimized connection string settings for ODP.NET
- **Automatic Schema Management**: Optional automatic database schema creation
- **Configurable Options**: Extensive configuration options for customization

## Requirements

- .NET 8.0 or .NET 10.0
- **Oracle Database 19c or later** (recommended)
  - Oracle 12c-18c supported with `UseSkipLocked = false`
- Hangfire.Core 1.8.x

## Installation

Add the package reference to your project:

```xml
<PackageReference Include="DevDad.Hangfire.Oracle" Version="1.0.0" />
```

Or via Package Manager Console:

```powershell
Install-Package DevDad.Hangfire.Oracle
```

## Usage

### Basic Setup

```csharp
using Hangfire;
using DevDad.Hangfire.Oracle;

// Configure Hangfire to use Oracle storage
GlobalConfiguration.Configuration
    .UseOracleStorage("Data Source=myOracleDB;User Id=hangfire;Password=yourpassword;");
```

### Advanced Configuration

```csharp
using Hangfire;
using DevDad.Hangfire.Oracle;

var options = new OracleStorageOptions
{
    // Schema settings
    SchemaName = "HANGFIRE",              // Optional schema name
    TablePrefix = "HF_",                   // Table name prefix (default: "HF_")
    PrepareSchemaIfNecessary = true,       // Auto-create schema
    
    // Oracle 19c+ optimizations
    MinimumDatabaseVersion = OracleDatabaseVersion.Oracle19c,
    UseSkipLocked = true,                  // Use FOR UPDATE SKIP LOCKED (Oracle 19c+)
    
    // Timeouts and intervals
    InvisibilityTimeout = TimeSpan.FromMinutes(30),
    QueuePollInterval = TimeSpan.FromSeconds(15),
    DistributedLockTimeout = TimeSpan.FromMinutes(10),
    JobExpirationCheckInterval = TimeSpan.FromMinutes(30),
    CounterAggregationInterval = TimeSpan.FromMinutes(5),
    SlidingInvisibilityTimeout = TimeSpan.FromMinutes(5),
    
    // Performance settings
    CommandTimeout = 30,                   // SQL command timeout in seconds
    MaxRetryAttempts = 3,                  // Retry attempts for transient errors
    RetryDelay = TimeSpan.FromMilliseconds(100),
    CleanupBatchSize = 1000,               // Batch size for cleanup operations
    EnableStatementCaching = true,         // Enable ODP.NET statement caching
    StatementCacheSize = 100,              // Statement cache size
    
    // Other settings
    FetchCount = 1,                        // Jobs to fetch per query
    UseUtcTime = true                      // Use UTC timestamps
};

GlobalConfiguration.Configuration
    .UseOracleStorage("YourConnectionString", options);
```

### ASP.NET Core Integration

```csharp
using Hangfire;
using DevDad.Hangfire.Oracle;

var builder = WebApplication.CreateBuilder(args);

// Add Hangfire services
builder.Services.AddHangfire(configuration => configuration
    .SetDataCompatibilityLevel(CompatibilityLevel.Version_180)
    .UseSimpleAssemblyNameTypeSerializer()
    .UseRecommendedSerializerSettings()
    .UseOracleStorage(
        builder.Configuration.GetConnectionString("HangfireOracle"),
        new OracleStorageOptions
        {
            PrepareSchemaIfNecessary = true,
            MinimumDatabaseVersion = OracleDatabaseVersion.Oracle19c,
            UseSkipLocked = true
        }));

builder.Services.AddHangfireServer();

var app = builder.Build();

app.UseHangfireDashboard();
app.Run();
```

### Connection String Best Practices

The storage automatically enhances connection strings with optimal pooling settings:

```csharp
// Recommended connection string format
var connectionString = @"
    Data Source=myOracleDB;
    User Id=hangfire;
    Password=yourpassword;
    Min Pool Size=5;
    Max Pool Size=100;
    Connection Lifetime=120;
    Statement Cache Size=100;
";
```

## Database Schema

The package will automatically create the required database schema if `PrepareSchemaIfNecessary` is set to `true` (default).

The following tables are created:

- `HF_JOB` - Stores job definitions
- `HF_JOB_STATE` - Stores job state history
- `HF_JOB_PARAMETER` - Stores job parameters
- `HF_JOB_QUEUE` - Stores job queue assignments
- `HF_SERVER` - Stores registered servers
- `HF_SET` - Stores sorted sets (scheduled jobs, etc.)
- `HF_COUNTER` - Stores counters
- `HF_HASH` - Stores key-value pairs
- `HF_LIST` - Stores lists
- `HF_AGGREGATED_COUNTER` - Stores aggregated counters
- `HF_DISTRIBUTED_LOCK` - Manages distributed locks

### Manual Schema Installation

If you prefer to create the schema manually, you can run the SQL scripts located in the `Scripts` folder:

1. **Install.sql** - Creates all required tables and sequences
2. **Uninstall.sql** - Drops all Hangfire tables and sequences

## Configuration Options

| Option | Type | Default | Description |
| ------ | ---- | ------- | ----------- |
| `SchemaName` | `string?` | `null` | Optional Oracle schema name |
| `TablePrefix` | `string` | `"HF_"` | Prefix for all table names |
| `MinimumDatabaseVersion` | `OracleDatabaseVersion` | `Oracle19c` | Minimum Oracle version to target |
| `UseSkipLocked` | `bool` | `true` | Use FOR UPDATE SKIP LOCKED (Oracle 19c+) |
| `InvisibilityTimeout` | `TimeSpan` | 30 minutes | Timeout for fetched jobs |
| `QueuePollInterval` | `TimeSpan` | 15 seconds | Interval between queue polls |
| `DistributedLockTimeout` | `TimeSpan` | 10 minutes | Timeout for distributed locks |
| `JobExpirationCheckInterval` | `TimeSpan` | 30 minutes | Interval for job expiration checks |
| `CounterAggregationInterval` | `TimeSpan` | 5 minutes | Interval for counter aggregation |
| `PrepareSchemaIfNecessary` | `bool` | `true` | Auto-create database schema |
| `SlidingInvisibilityTimeout` | `TimeSpan` | 5 minutes | Sliding timeout for jobs |
| `FetchCount` | `int` | `1` | Number of jobs to fetch per query |
| `UseUtcTime` | `bool` | `true` | Use UTC for all timestamps |
| `CommandTimeout` | `int` | `30` | SQL command timeout in seconds |
| `MaxRetryAttempts` | `int` | `3` | Retry attempts for transient errors |
| `RetryDelay` | `TimeSpan` | 100ms | Base delay between retries |
| `CleanupBatchSize` | `int` | `1000` | Batch size for cleanup operations |
| `EnableStatementCaching` | `bool` | `true` | Enable ODP.NET statement caching |
| `StatementCacheSize` | `int` | `100` | Number of statements to cache |

## Oracle Version Compatibility

| Oracle Version | Supported | Notes |
| -------------- | --------- | ----- |
| Oracle 19c+ | ✅ Full | All features including SKIP LOCKED |
| Oracle 21c+ | ✅ Full | All features |
| Oracle 23ai | ✅ Full | All features |
| Oracle 12c-18c | ⚠️ Limited | Set `UseSkipLocked = false` |

## Dependencies

- Hangfire.Core 1.8.22
- Oracle.ManagedDataAccess.Core 23.26.100
- Dapper 2.1.66
- Dapper.Oracle 2.0.2

## License

This project is licensed under the MIT License.

## Contributing

Contributions are welcome! Please feel free to submit pull requests or open issues.

## Support

For issues and questions, please use the [GitHub Issues](https://github.com/wouternijenhuis/Hangfire.Storage.Oracle/issues) page.
