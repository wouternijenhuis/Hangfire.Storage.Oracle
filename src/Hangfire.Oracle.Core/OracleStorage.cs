using System.Data;
using Dapper;
using Hangfire.Logging;
using Hangfire.Oracle.Core.BackgroundProcesses;
using Hangfire.Oracle.Core.Queue;
using Hangfire.Oracle.Core.Schema;
using Hangfire.Server;
using Hangfire.Storage;
using Oracle.ManagedDataAccess.Client;

namespace Hangfire.Oracle.Core;

/// <summary>
/// Oracle storage implementation for Hangfire.
/// Provides job storage, queue management, and monitoring capabilities using Oracle database.
/// </summary>
/// <remarks>
/// <para>
/// This implementation is optimized for Oracle Database 19c and higher.
/// Key features include:
/// </para>
/// <list type="bullet">
///   <item><description>Non-blocking job queue with <c>FOR UPDATE SKIP LOCKED</c></description></item>
///   <item><description>Automatic retry with exponential backoff for transient failures</description></item>
///   <item><description>Connection pooling optimization with statement caching</description></item>
///   <item><description>Background processes for counter aggregation and cleanup</description></item>
/// </list>
/// <para>
/// For optimal performance, configure your connection string with appropriate pooling settings:
/// <c>Min Pool Size=5;Max Pool Size=100;Connection Lifetime=120;</c>
/// </para>
/// </remarks>
public class OracleStorage : JobStorage, IDisposable
{
    private static readonly ILog _logger = LogProvider.GetLogger(typeof(OracleStorage));

    private readonly string _connectionString;
    private readonly OracleStorageOptions _options;
    private readonly Lazy<JobQueueProviderCollection> _queueProviders;
    private readonly SemaphoreSlim _connectionSemaphore;
    private string? _displayName;
    private bool _disposed;

    /// <summary>
    /// Initializes a new instance with the specified connection string using default options.
    /// </summary>
    /// <param name="connectionString">Oracle database connection string.</param>
    /// <exception cref="ArgumentNullException">When connection string is null or empty.</exception>
    public OracleStorage(string connectionString)
        : this(connectionString, new OracleStorageOptions())
    {
    }

    /// <summary>
    /// Initializes a new instance with the specified connection string and options.
    /// </summary>
    /// <param name="connectionString">Oracle database connection string.</param>
    /// <param name="options">Storage configuration options.</param>
    /// <exception cref="ArgumentNullException">When connection string or options is null.</exception>
    public OracleStorage(string connectionString, OracleStorageOptions options)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            throw new ArgumentNullException(nameof(connectionString));
        }

        _connectionString = EnhanceConnectionString(connectionString, options);
        _options = options ?? throw new ArgumentNullException(nameof(options));

        // Limit concurrent connection creation during bursts
        _connectionSemaphore = new SemaphoreSlim(50, 50);

        _queueProviders = new Lazy<JobQueueProviderCollection>(
            () => new JobQueueProviderCollection(new OracleJobQueueProvider(this, _options)));

        if (_options.PrepareSchemaIfNecessary)
        {
            InitializeSchema();
        }

        _logger.InfoFormat(
            "Hangfire Oracle storage initialized. Target: Oracle {0}+, Schema: {1}",
            (int)_options.MinimumDatabaseVersion,
            _options.SchemaName ?? "(default)");
    }

    /// <summary>
    /// Gets the storage configuration options.
    /// </summary>
    public OracleStorageOptions Options => _options;

    /// <summary>
    /// Gets the database connection string.
    /// </summary>
    public string ConnectionString => _connectionString;

    /// <summary>
    /// Gets the collection of queue providers.
    /// </summary>
    public JobQueueProviderCollection QueueProviders => _queueProviders.Value;

    /// <summary>
    /// Creates a new storage connection for job operations.
    /// </summary>
    public override IStorageConnection GetConnection()
    {
        return new OracleStorageConnection(this);
    }

    /// <summary>
    /// Creates a new monitoring API instance for dashboard support.
    /// </summary>
    public override IMonitoringApi GetMonitoringApi()
    {
        return new OracleMonitoringApi(this);
    }

    /// <summary>
    /// Gets the background server components for maintenance operations.
    /// </summary>
#pragma warning disable CS0618 // IServerComponent is obsolete but required for Hangfire
    [Obsolete]
    public override IEnumerable<IServerComponent> GetComponents()
#pragma warning restore CS0618
    {
        yield return new CounterAggregationProcess(
            this,
            _options.CounterAggregationInterval,
            _options.CleanupBatchSize);

        yield return new ExpiredRecordsCleanupProcess(
            this,
            _options.JobExpirationCheckInterval,
            _options.CleanupBatchSize);
    }

    /// <summary>
    /// Writes storage configuration to the log.
    /// </summary>
    public override void WriteOptionsToLog(ILog logger)
    {
        logger.Info("Using Oracle storage for Hangfire with the following options:");
        logger.InfoFormat("    Minimum Oracle Version: {0}", _options.MinimumDatabaseVersion);
        logger.InfoFormat("    Schema: {0}", _options.SchemaName ?? "(default)");
        logger.InfoFormat("    Table prefix: {0}", _options.TablePrefix);
        logger.InfoFormat("    Queue poll interval: {0}", _options.QueuePollInterval);
        logger.InfoFormat("    Invisibility timeout: {0}", _options.InvisibilityTimeout);
        logger.InfoFormat("    Job expiration check: {0}", _options.JobExpirationCheckInterval);
        logger.InfoFormat("    Counter aggregation: {0}", _options.CounterAggregationInterval);
        logger.InfoFormat("    Use SKIP LOCKED: {0}", _options.UseSkipLocked);
        logger.InfoFormat("    Command timeout: {0}s", _options.CommandTimeout);
        logger.InfoFormat("    Max retry attempts: {0}", _options.MaxRetryAttempts);
    }

    /// <summary>
    /// Returns a display name for this storage instance.
    /// </summary>
    public override string ToString()
    {
        if (_displayName != null)
        {
            return _displayName;
        }

        _displayName = BuildDisplayName();
        return _displayName;
    }

    /// <summary>
    /// Creates and opens a new Oracle database connection.
    /// </summary>
    /// <remarks>
    /// The connection is configured with statement caching and the appropriate schema
    /// based on the storage options. Connection pooling is managed by ODP.NET.
    /// </remarks>
    internal OracleConnection CreateAndOpenConnection()
    {
        // Brief wait to prevent connection storms
        if (!_connectionSemaphore.Wait(TimeSpan.FromSeconds(30)))
        {
            throw new TimeoutException("Timeout waiting for connection slot.");
        }

        try
        {
            var connection = new OracleConnection(_connectionString);
            connection.Open();

            // Set schema if configured
            if (!string.IsNullOrWhiteSpace(_options.SchemaName))
            {
                using var command = connection.CreateCommand();
                command.CommandText = $"ALTER SESSION SET CURRENT_SCHEMA = {_options.SchemaName}";
                command.CommandTimeout = _options.CommandTimeout;
                command.ExecuteNonQuery();
            }

            return connection;
        }
        finally
        {
            _connectionSemaphore.Release();
        }
    }

    /// <summary>
    /// Creates and opens a new Oracle database connection asynchronously.
    /// </summary>
    internal async Task<OracleConnection> CreateAndOpenConnectionAsync(CancellationToken cancellationToken = default)
    {
        if (!await _connectionSemaphore.WaitAsync(TimeSpan.FromSeconds(30), cancellationToken).ConfigureAwait(false))
        {
            throw new TimeoutException("Timeout waiting for connection slot.");
        }

        try
        {
            var connection = new OracleConnection(_connectionString);

            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

            if (!string.IsNullOrWhiteSpace(_options.SchemaName))
            {
                await connection.ExecuteAsync(
                    $"ALTER SESSION SET CURRENT_SCHEMA = {_options.SchemaName}",
                    commandTimeout: _options.CommandTimeout).ConfigureAwait(false);
            }

            return connection;
        }
        finally
        {
            _connectionSemaphore.Release();
        }
    }

    /// <summary>
    /// Executes an operation within a database connection context.
    /// </summary>
    internal T ExecuteWithConnection<T>(Func<OracleConnection, T> operation)
    {
        using var connection = CreateAndOpenConnection();
        return operation(connection);
    }

    /// <summary>
    /// Executes an operation within a database transaction.
    /// </summary>
    internal T ExecuteInTransaction<T>(
        Func<OracleConnection, IDbTransaction, T> operation,
        IsolationLevel? isolationLevel = null)
    {
        using var connection = CreateAndOpenConnection();
        using var transaction = connection.BeginTransaction(
            isolationLevel ?? _options.TransactionIsolationLevel);

        try
        {
            var result = operation(connection, transaction);
            transaction.Commit();
            return result;
        }
        catch
        {
            transaction.Rollback();
            throw;
        }
    }

    /// <summary>
    /// Executes an operation within a database transaction (void return).
    /// </summary>
    internal void ExecuteInTransaction(
        Action<OracleConnection, IDbTransaction> operation,
        IsolationLevel? isolationLevel = null)
    {
        ExecuteInTransaction((conn, tx) =>
        {
            operation(conn, tx);
            return 0;
        }, isolationLevel);
    }

    /// <summary>
    /// Gets the fully qualified table name with optional schema and prefix.
    /// </summary>
    internal string GetTableName(string tableName)
    {
        var prefix = _options.TablePrefix ?? "HF_";
        var fullTableName = $"{prefix}{tableName}";

        if (!string.IsNullOrWhiteSpace(_options.SchemaName))
        {
            return $"{_options.SchemaName}.{fullTableName}";
        }

        return fullTableName;
    }

    /// <summary>
    /// Executes an async operation within a database connection context.
    /// </summary>
    internal async Task<T> ExecuteWithConnectionAsync<T>(
        Func<OracleConnection, Task<T>> operation,
        CancellationToken cancellationToken = default)
    {
        var connection = await CreateAndOpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using (connection.ConfigureAwait(false))
        {
            return await operation(connection).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Executes an async operation within a database transaction.
    /// </summary>
    internal async Task<T> ExecuteInTransactionAsync<T>(
        Func<OracleConnection, IDbTransaction, Task<T>> operation,
        IsolationLevel? isolationLevel = null,
        CancellationToken cancellationToken = default)
    {
        var connection = await CreateAndOpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using (connection.ConfigureAwait(false))
        {
            using var transaction = connection.BeginTransaction(
                isolationLevel ?? _options.TransactionIsolationLevel);

            try
            {
                var result = await operation(connection, transaction).ConfigureAwait(false);
                transaction.Commit();
                return result;
            }
            catch
            {
                transaction.Rollback();
                throw;
            }
        }
    }

    /// <summary>
    /// Releases resources used by the storage.
    /// </summary>
    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    /// <summary>
    /// Releases resources used by the storage.
    /// </summary>
    protected virtual void Dispose(bool disposing)
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;

        if (disposing)
        {
            _connectionSemaphore.Dispose();
        }
    }

    private void InitializeSchema()
    {
        using var connection = CreateAndOpenConnection();
        OracleSchemaManager.EnsureSchemaCreated(connection, _options.TablePrefix, _options.SchemaName);
    }

    private string BuildDisplayName()
    {
        try
        {
            // Extract meaningful info from connection string for display
            var builder = new OracleConnectionStringBuilder(_connectionString);
            var dataSource = builder.DataSource;

            return string.IsNullOrEmpty(dataSource)
                ? "Hangfire.Oracle.Core"
                : $"Hangfire.Oracle.Core: {dataSource}";
        }
        catch
        {
            return "Hangfire.Oracle.Core";
        }
    }

    /// <summary>
    /// Enhances the connection string with ODP.NET best practice settings.
    /// </summary>
    private static string EnhanceConnectionString(string connectionString, OracleStorageOptions options)
    {
        try
        {
            var builder = new OracleConnectionStringBuilder(connectionString);

            // Only set defaults if not already specified
            // Connection pooling optimization for Hangfire's usage pattern
            if (!builder.TryGetValue("Min Pool Size", out _))
            {
                builder.MinPoolSize = 5;
            }

            if (!builder.TryGetValue("Max Pool Size", out _))
            {
                builder.MaxPoolSize = 100;
            }

            if (!builder.TryGetValue("Connection Lifetime", out _))
            {
                builder.ConnectionLifeTime = 120;
            }

            if (!builder.TryGetValue("Connection Timeout", out _))
            {
                builder.ConnectionTimeout = options.CommandTimeout;
            }

            // Enable self-tuning for Oracle 19c+
            if (!builder.TryGetValue("Self Tuning", out _))
            {
                builder["Self Tuning"] = true;
            }

            // Set statement cache size for better prepared statement reuse
            if (options.EnableStatementCaching && !builder.TryGetValue("Statement Cache Size", out _))
            {
                builder["Statement Cache Size"] = options.StatementCacheSize;
            }

            return builder.ConnectionString;
        }
        catch (Exception ex)
        {
            _logger.WarnFormat("Could not enhance connection string: {0}", ex.Message);
            return connectionString;
        }
    }
}
