# Hangfire.Oracle.Core

A Hangfire storage provider implementation for Oracle Database using Oracle.ManagedDataAccess.Core and Dapper.

## Features

- **Full Hangfire Storage Implementation**: Implements all required Hangfire storage interfaces
  - `OracleStorage` : `JobStorage`
  - `IStorageConnection` for read operations
  - `IWriteOnlyTransaction` for write operations
  - `IMonitoringApi` for dashboard and monitoring

- **Oracle Database Support**: Uses Oracle.ManagedDataAccess.Core for reliable Oracle connectivity
- **High Performance**: Leverages Dapper for efficient data access
- **Distributed Locks**: Ensures job processing coordination across multiple servers
- **Queue Polling**: Efficient job queue management with configurable polling intervals
- **Automatic Schema Management**: Optional automatic database schema creation
- **Configurable Options**: Extensive configuration options for customization

## Installation

Add the package reference to your project:

```xml
<PackageReference Include="Hangfire.Oracle.Core" Version="1.0.0" />
```

Or via Package Manager Console:

```powershell
Install-Package Hangfire.Oracle.Core
```

## Usage

### Basic Setup

```csharp
using Hangfire;
using Hangfire.Oracle.Core;

// Configure Hangfire to use Oracle storage
GlobalConfiguration.Configuration
    .UseOracleStorage("Data Source=myOracleDB;User Id=hangfire;Password=yourpassword;");
```

### Advanced Configuration

```csharp
using Hangfire;
using Hangfire.Oracle.Core;

var options = new OracleStorageOptions
{
    SchemaName = "HANGFIRE",              // Optional schema name
    TablePrefix = "HF_",                   // Table name prefix (default: "HF_")
    InvisibilityTimeout = TimeSpan.FromMinutes(30),
    QueuePollInterval = TimeSpan.FromSeconds(15),
    DistributedLockTimeout = TimeSpan.FromMinutes(10),
    JobExpirationCheckInterval = TimeSpan.FromMinutes(30),
    CounterAggregationInterval = TimeSpan.FromMinutes(5),
    PrepareSchemaIfNecessary = true,       // Auto-create schema
    SlidingInvisibilityTimeout = TimeSpan.FromMinutes(5),
    FetchCount = 1,                        // Jobs to fetch per query
    UseUtcTime = true                      // Use UTC timestamps
};

GlobalConfiguration.Configuration
    .UseOracleStorage("YourConnectionString", options);
```

### ASP.NET Core Integration

```csharp
using Hangfire;
using Hangfire.Oracle.Core;

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
            PrepareSchemaIfNecessary = true
        }));

builder.Services.AddHangfireServer();

var app = builder.Build();

app.UseHangfireDashboard();
app.Run();
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
|--------|------|---------|-------------|
| `SchemaName` | `string?` | `null` | Optional Oracle schema name |
| `TablePrefix` | `string` | `"HF_"` | Prefix for all table names |
| `InvisibilityTimeout` | `TimeSpan` | 30 minutes | Timeout for fetched jobs |
| `QueuePollInterval` | `TimeSpan` | 15 seconds | Interval between queue polls |
| `DistributedLockTimeout` | `TimeSpan` | 10 minutes | Timeout for distributed locks |
| `JobExpirationCheckInterval` | `TimeSpan` | 30 minutes | Interval for job expiration checks |
| `CounterAggregationInterval` | `TimeSpan` | 5 minutes | Interval for counter aggregation |
| `PrepareSchemaIfNecessary` | `bool` | `true` | Auto-create database schema |
| `SlidingInvisibilityTimeout` | `TimeSpan` | 5 minutes | Sliding timeout for jobs |
| `FetchCount` | `int` | `1` | Number of jobs to fetch per query |
| `UseUtcTime` | `bool` | `true` | Use UTC for all timestamps |

## Requirements

- .NET 8.0 or later
- Oracle Database 12c or later
- Hangfire.Core 1.8.x

## Dependencies

- Hangfire.Core (≥ 1.8.22)
- Oracle.ManagedDataAccess.Core (≥ 23.26.100)
- Dapper (≥ 2.1.66)
- Dapper.Oracle (≥ 2.0.2)

## License

This project is licensed under the MIT License.

## Contributing

Contributions are welcome! Please feel free to submit pull requests or open issues.

## Support

For issues and questions, please use the [GitHub Issues](https://github.com/wouternijenhuis/Hangfire.Storage.Oracle/issues) page.
