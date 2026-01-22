using System;
using System.Data;

namespace Hangfire.Oracle.Core;

/// <summary>
/// Configuration options for Oracle storage.
/// Provides fine-grained control over connection behavior, queue processing,
/// background maintenance, and database schema settings.
/// </summary>
public class OracleStorageOptions
{
    /// <summary>
    /// Gets or sets the database schema name.
    /// When set, all table operations will be prefixed with this schema.
    /// Default is null (uses the connection's default schema).
    /// </summary>
    public string? SchemaName { get; set; }

    /// <summary>
    /// Gets or sets the transaction isolation level for database operations.
    /// Default is ReadCommitted.
    /// </summary>
    public IsolationLevel TransactionIsolationLevel { get; set; } = IsolationLevel.ReadCommitted;

    /// <summary>
    /// Gets or sets the maximum number of jobs displayed in dashboard lists.
    /// Higher values may impact dashboard performance.
    /// Default is 50000.
    /// </summary>
    public int DashboardJobListLimit { get; set; } = 50000;

    /// <summary>
    /// Gets or sets the timeout for database transactions.
    /// Long-running operations will be aborted after this period.
    /// Default is 1 minute.
    /// </summary>
    public TimeSpan TransactionTimeout { get; set; } = TimeSpan.FromMinutes(1);

    /// <summary>
    /// Gets or sets the table name prefix.
    /// Default is "HF_".
    /// </summary>
    public string TablePrefix { get; set; } = "HF_";

    /// <summary>
    /// Gets or sets the invisibility timeout for queue polling.
    /// Default is 30 minutes.
    /// </summary>
    public TimeSpan InvisibilityTimeout { get; set; } = TimeSpan.FromMinutes(30);

    /// <summary>
    /// Gets or sets the queue poll interval.
    /// Default is 15 seconds.
    /// </summary>
    public TimeSpan QueuePollInterval { get; set; } = TimeSpan.FromSeconds(15);

    /// <summary>
    /// Gets or sets the distributed lock timeout.
    /// Default is 10 minutes.
    /// </summary>
    public TimeSpan DistributedLockTimeout { get; set; } = TimeSpan.FromMinutes(10);

    /// <summary>
    /// Gets or sets the job expiration check interval.
    /// Default is 30 minutes.
    /// </summary>
    public TimeSpan JobExpirationCheckInterval { get; set; } = TimeSpan.FromMinutes(30);

    /// <summary>
    /// Gets or sets the counter aggregation interval.
    /// Default is 5 minutes.
    /// </summary>
    public TimeSpan CounterAggregationInterval { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// Gets or sets whether to prepare the database schema automatically.
    /// Default is true.
    /// </summary>
    public bool PrepareSchemaIfNecessary { get; set; } = true;

    /// <summary>
    /// Gets or sets the sliding invisibility timeout for fetched jobs.
    /// Default is 5 minutes.
    /// </summary>
    public TimeSpan SlidingInvisibilityTimeout { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// Gets or sets the number of records to fetch from the queue.
    /// Default is 1.
    /// </summary>
    public int FetchCount { get; set; } = 1;

    /// <summary>
    /// Gets or sets whether to use UTC time.
    /// Default is true.
    /// </summary>
    public bool UseUtcTime { get; set; } = true;
}
