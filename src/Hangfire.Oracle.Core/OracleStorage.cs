using System;
using System.Collections.Generic;
using System.Data;
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
public class OracleStorage : JobStorage, IDisposable
{
    private static readonly ILog Logger = LogProvider.GetLogger(typeof(OracleStorage));

    private readonly string _connectionString;
    private readonly OracleStorageOptions _options;
    private readonly Lazy<JobQueueProviderCollection> _queueProviders;
    private string? _displayName;
    private bool _disposed;

    /// <summary>
    /// Initializes a new instance with the specified connection string using default options.
    /// </summary>
    /// <param name="connectionString">Oracle database connection string.</param>
    public OracleStorage(string connectionString)
        : this(connectionString, new OracleStorageOptions())
    {
    }

    /// <summary>
    /// Initializes a new instance with the specified connection string and options.
    /// </summary>
    /// <param name="connectionString">Oracle database connection string.</param>
    /// <param name="options">Storage configuration options.</param>
    public OracleStorage(string connectionString, OracleStorageOptions options)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
            throw new ArgumentNullException(nameof(connectionString));

        _connectionString = connectionString;
        _options = options ?? throw new ArgumentNullException(nameof(options));

        _queueProviders = new Lazy<JobQueueProviderCollection>(
            () => new JobQueueProviderCollection(new OracleJobQueueProvider(this, _options)));

        if (_options.PrepareSchemaIfNecessary)
        {
            InitializeSchema();
        }
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
    public override IEnumerable<IServerComponent> GetComponents()
#pragma warning restore CS0618
    {
        yield return new CounterAggregationProcess(this, _options.CounterAggregationInterval);
        yield return new ExpiredRecordsCleanupProcess(this, _options.JobExpirationCheckInterval);
    }

    /// <summary>
    /// Writes storage configuration to the log.
    /// </summary>
    public override void WriteOptionsToLog(ILog logger)
    {
        logger.Info("Using Oracle storage for Hangfire with the following options:");
        logger.InfoFormat("    Schema: {0}", _options.SchemaName ?? "(default)");
        logger.InfoFormat("    Table prefix: {0}", _options.TablePrefix);
        logger.InfoFormat("    Queue poll interval: {0}", _options.QueuePollInterval);
        logger.InfoFormat("    Invisibility timeout: {0}", _options.InvisibilityTimeout);
        logger.InfoFormat("    Job expiration check: {0}", _options.JobExpirationCheckInterval);
        logger.InfoFormat("    Counter aggregation: {0}", _options.CounterAggregationInterval);
    }

    /// <summary>
    /// Returns a display name for this storage instance.
    /// </summary>
    public override string ToString()
    {
        if (_displayName != null)
            return _displayName;

        _displayName = BuildDisplayName();
        return _displayName;
    }

    /// <summary>
    /// Creates and opens a new Oracle database connection.
    /// </summary>
    internal OracleConnection CreateAndOpenConnection()
    {
        var connection = new OracleConnection(_connectionString);
        connection.Open();

        // Set schema if configured
        if (!string.IsNullOrWhiteSpace(_options.SchemaName))
        {
            using var command = connection.CreateCommand();
            command.CommandText = $"ALTER SESSION SET CURRENT_SCHEMA = {_options.SchemaName}";
            command.ExecuteNonQuery();
        }

        return connection;
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
        if (_disposed) return;
        _disposed = true;

        // Clean up any resources if needed
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
}
