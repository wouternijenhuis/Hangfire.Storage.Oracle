using System.Data;

namespace Hangfire.Oracle.Core;

/// <summary>
/// Configuration options for Oracle storage.
/// Provides fine-grained control over connection behavior, queue processing,
/// background maintenance, and database schema settings.
/// </summary>
/// <remarks>
/// <para>
/// This class supports Oracle Database 19c and higher. Some features like
/// <c>FOR UPDATE SKIP LOCKED</c> require Oracle 19c+.
/// </para>
/// <para>
/// For optimal performance, ensure your connection string includes appropriate
/// pooling settings such as <c>Min Pool Size</c>, <c>Max Pool Size</c>, and
/// <c>Connection Lifetime</c>.
/// </para>
/// </remarks>
public class OracleStorageOptions
{
    // ============================================================
    // Schema and Table Configuration
    // ============================================================

    /// <summary>
    /// Gets or sets the database schema name.
    /// When set, all table operations will be prefixed with this schema.
    /// Default is <c>null</c> (uses the connection's default schema).
    /// </summary>
    /// <example>
    /// <code>
    /// options.SchemaName = "HANGFIRE";
    /// // Tables will be accessed as HANGFIRE.HF_JOB, HANGFIRE.HF_SERVER, etc.
    /// </code>
    /// </example>
    public string? SchemaName { get; set; }

    /// <summary>
    /// Gets or sets the table name prefix.
    /// Default is <c>"HF_"</c>.
    /// </summary>
    /// <remarks>
    /// Changing this value requires corresponding changes to the database schema.
    /// The prefix is prepended to all Hangfire table names (JOB, SERVER, SET, etc.).
    /// </remarks>
    public string TablePrefix { get; set; } = "HF_";

    // ============================================================
    // Transaction and Connection Settings
    // ============================================================

    /// <summary>
    /// Gets or sets the transaction isolation level for database operations.
    /// Default is <see cref="IsolationLevel.ReadCommitted"/>.
    /// </summary>
    /// <remarks>
    /// Oracle recommends <see cref="IsolationLevel.ReadCommitted"/> for most OLTP workloads.
    /// Avoid <see cref="IsolationLevel.Serializable"/> unless absolutely required, as it can
    /// cause significant lock contention.
    /// </remarks>
    public IsolationLevel TransactionIsolationLevel { get; set; } = IsolationLevel.ReadCommitted;

    /// <summary>
    /// Gets or sets the timeout for database transactions.
    /// Long-running operations will be aborted after this period.
    /// Default is 1 minute.
    /// </summary>
    public TimeSpan TransactionTimeout { get; set; } = TimeSpan.FromMinutes(1);

    /// <summary>
    /// Gets or sets the command timeout for individual SQL commands in seconds.
    /// Default is 30 seconds.
    /// </summary>
    /// <remarks>
    /// Set to 0 for no timeout. This applies to individual SQL commands, not transactions.
    /// For long-running operations, consider increasing both this and <see cref="TransactionTimeout"/>.
    /// </remarks>
    public int CommandTimeout { get; set; } = 30;

    /// <summary>
    /// Gets or sets the maximum number of retry attempts for transient database failures.
    /// Default is 3.
    /// </summary>
    /// <remarks>
    /// Transient failures include ORA-00060 (deadlock), ORA-00054 (resource busy),
    /// ORA-12170 (connect timeout), and other recoverable errors.
    /// </remarks>
    public int MaxRetryAttempts { get; set; } = 3;

    /// <summary>
    /// Gets or sets the base delay between retry attempts.
    /// The actual delay uses exponential backoff: delay * 2^attempt.
    /// Default is 100 milliseconds.
    /// </summary>
    public TimeSpan RetryDelay { get; set; } = TimeSpan.FromMilliseconds(100);

    // ============================================================
    // Queue Processing Settings
    // ============================================================

    /// <summary>
    /// Gets or sets the invisibility timeout for queue polling.
    /// Jobs not completed within this time become visible again for reprocessing.
    /// Default is 30 minutes.
    /// </summary>
    /// <remarks>
    /// Set this value higher than the expected maximum job execution time
    /// to prevent duplicate processing.
    /// </remarks>
    public TimeSpan InvisibilityTimeout { get; set; } = TimeSpan.FromMinutes(30);

    /// <summary>
    /// Gets or sets the queue poll interval.
    /// How often workers check for new jobs when queues are empty.
    /// Default is 15 seconds.
    /// </summary>
    /// <remarks>
    /// Lower values provide faster job pickup but increase database load.
    /// Values below 1 second are not recommended for production.
    /// </remarks>
    public TimeSpan QueuePollInterval { get; set; } = TimeSpan.FromSeconds(15);

    /// <summary>
    /// Gets or sets the sliding invisibility timeout for fetched jobs.
    /// Used for job heartbeat extension during long-running operations.
    /// Default is 5 minutes.
    /// </summary>
    public TimeSpan SlidingInvisibilityTimeout { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// Gets or sets the number of jobs to fetch from the queue per poll.
    /// Default is 1.
    /// </summary>
    /// <remarks>
    /// Higher values reduce database round-trips but may cause uneven work distribution.
    /// For high-throughput scenarios, consider values between 1-10.
    /// </remarks>
    public int FetchCount { get; set; } = 1;

    /// <summary>
    /// Gets or sets whether to use <c>FOR UPDATE SKIP LOCKED</c> for queue operations.
    /// Requires Oracle 19c or higher. Default is <c>true</c>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <c>SKIP LOCKED</c> allows multiple workers to dequeue jobs concurrently without
    /// blocking. This significantly improves throughput in high-concurrency scenarios.
    /// </para>
    /// <para>
    /// If targeting Oracle 12c-18c, set this to <c>false</c> to use alternative locking.
    /// </para>
    /// </remarks>
    public bool UseSkipLocked { get; set; } = true;

    // ============================================================
    // Distributed Lock Settings
    // ============================================================

    /// <summary>
    /// Gets or sets the distributed lock timeout.
    /// Maximum time to wait when acquiring a distributed lock.
    /// Default is 10 minutes.
    /// </summary>
    public TimeSpan DistributedLockTimeout { get; set; } = TimeSpan.FromMinutes(10);

    /// <summary>
    /// Gets or sets whether to use Oracle DBMS_LOCK for distributed locking.
    /// Requires EXECUTE privilege on DBMS_LOCK. Default is <c>false</c>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// DBMS_LOCK provides more robust distributed locking than table-based locks,
    /// with better deadlock detection and automatic cleanup.
    /// </para>
    /// <para>
    /// When <c>false</c>, uses a table-based locking mechanism that doesn't require
    /// additional privileges but may be slightly less efficient.
    /// </para>
    /// </remarks>
    public bool UseDbmsLock { get; set; }

    // ============================================================
    // Background Process Settings
    // ============================================================

    /// <summary>
    /// Gets or sets the job expiration check interval.
    /// How often to clean up expired jobs and related data.
    /// Default is 30 minutes.
    /// </summary>
    public TimeSpan JobExpirationCheckInterval { get; set; } = TimeSpan.FromMinutes(30);

    /// <summary>
    /// Gets or sets the counter aggregation interval.
    /// How often to merge individual counter records into aggregated counters.
    /// Default is 5 minutes.
    /// </summary>
    /// <remarks>
    /// Counter aggregation reduces table size and improves dashboard query performance.
    /// More frequent aggregation keeps the counter table smaller but increases CPU usage.
    /// </remarks>
    public TimeSpan CounterAggregationInterval { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// Gets or sets the number of records to process per batch during cleanup operations.
    /// Default is 1000.
    /// </summary>
    /// <remarks>
    /// Larger batches are more efficient but hold locks longer.
    /// Reduce this value if you experience lock contention during cleanup.
    /// </remarks>
    public int CleanupBatchSize { get; set; } = 1000;

    // ============================================================
    // Schema and Initialization Settings
    // ============================================================

    /// <summary>
    /// Gets or sets whether to prepare the database schema automatically.
    /// When <c>true</c>, creates tables, sequences, and indexes if they don't exist.
    /// Default is <c>true</c>.
    /// </summary>
    /// <remarks>
    /// Disable this in production if you prefer to manage schema changes manually
    /// or through database migration tools.
    /// </remarks>
    public bool PrepareSchemaIfNecessary { get; set; } = true;

    // ============================================================
    // Dashboard and Monitoring Settings
    // ============================================================

    /// <summary>
    /// Gets or sets the maximum number of jobs displayed in dashboard lists.
    /// Higher values may impact dashboard performance.
    /// Default is 50000.
    /// </summary>
    public int DashboardJobListLimit { get; set; } = 50000;

    // ============================================================
    // Time and Timezone Settings
    // ============================================================

    /// <summary>
    /// Gets or sets whether to use UTC time for all timestamps.
    /// Default is <c>true</c>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Using UTC is strongly recommended for distributed systems and when
    /// servers may be in different timezones.
    /// </para>
    /// <para>
    /// When <c>false</c>, uses the local time of the server, which can cause
    /// issues if servers have different timezone configurations.
    /// </para>
    /// </remarks>
    public bool UseUtcTime { get; set; } = true;

    // ============================================================
    // Oracle-Specific Performance Settings
    // ============================================================

    /// <summary>
    /// Gets or sets the minimum Oracle database version to target.
    /// This enables version-specific SQL optimizations.
    /// Default is <see cref="OracleDatabaseVersion.Oracle12c"/>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Setting this to <see cref="OracleDatabaseVersion.Oracle11g"/> uses only SQL
    /// features available in Oracle 11g (ROWNUM-based pagination, no SKIP LOCKED).
    /// </para>
    /// <para>
    /// Oracle 12c+ enables <c>FETCH FIRST</c> syntax. Oracle 19c+ enables <c>SKIP LOCKED</c>.
    /// </para>
    /// </remarks>
    public OracleDatabaseVersion MinimumDatabaseVersion { get; set; } = OracleDatabaseVersion.Oracle12c;

    /// <summary>
    /// Gets or sets whether to enable Oracle statement caching at the connection level.
    /// This improves performance by reusing parsed SQL statements.
    /// Default is <c>true</c>.
    /// </summary>
    /// <remarks>
    /// Statement caching requires additional memory on the Oracle server side.
    /// Disable if you experience memory pressure on the database server.
    /// </remarks>
    public bool EnableStatementCaching { get; set; } = true;

    /// <summary>
    /// Gets or sets the statement cache size per connection.
    /// Default is 50.
    /// </summary>
    /// <remarks>
    /// Increase this value if you observe frequent statement re-parsing.
    /// Monitor V$SQL_AREA to determine optimal cache size.
    /// </remarks>
    public int StatementCacheSize { get; set; } = 50;

    // ============================================================
    // Helper Properties (computed based on version)
    // ============================================================

    /// <summary>
    /// Gets whether the target Oracle version supports <c>FOR UPDATE SKIP LOCKED</c>.
    /// This feature is available in Oracle 19c and later.
    /// </summary>
    public bool SupportsSkipLocked => MinimumDatabaseVersion >= OracleDatabaseVersion.Oracle19c && UseSkipLocked;

    /// <summary>
    /// Gets whether the target Oracle version supports <c>FETCH FIRST</c> row limiting syntax.
    /// This feature is available in Oracle 12c and later.
    /// </summary>
    public bool SupportsFetchFirst => MinimumDatabaseVersion >= OracleDatabaseVersion.Oracle12c;

    /// <summary>
    /// Gets whether the target Oracle version supports partial indexes with WHERE clause.
    /// This feature is available in Oracle 12c and later.
    /// </summary>
    public bool SupportsPartialIndexes => MinimumDatabaseVersion >= OracleDatabaseVersion.Oracle12c;

    /// <summary>
    /// Gets whether the target Oracle version supports MERGE with DELETE clause.
    /// This feature is available in Oracle 10g and later.
    /// </summary>
    public bool SupportsMerge => true;
}

/// <summary>
/// Specifies the minimum Oracle database version to target.
/// This affects which SQL features and optimizations are used.
/// </summary>
public enum OracleDatabaseVersion
{
    /// <summary>
    /// Oracle Database 11g (11.x).
    /// Uses ROWNUM for pagination. No SKIP LOCKED support.
    /// </summary>
    Oracle11g = 11,

    /// <summary>
    /// Oracle Database 12c (12.x).
    /// Supports FETCH FIRST, partial indexes, and other 12c features.
    /// No SKIP LOCKED support.
    /// </summary>
    Oracle12c = 12,

    /// <summary>
    /// Oracle Database 18c (18.x).
    /// Similar feature set to 12c with incremental improvements.
    /// No SKIP LOCKED support.
    /// </summary>
    Oracle18c = 18,

    /// <summary>
    /// Oracle Database 19c (19.x) - Long Term Support release.
    /// Supports SKIP LOCKED, JSON, and other modern features.
    /// </summary>
    Oracle19c = 19,

    /// <summary>
    /// Oracle Database 21c (21.x).
    /// Adds native JSON data type and enhanced analytics.
    /// </summary>
    Oracle21c = 21,

    /// <summary>
    /// Oracle Database 23ai (23.x) - Innovation release.
    /// Adds JSON-relational duality, schema annotations, and more.
    /// </summary>
    Oracle23ai = 23
}
