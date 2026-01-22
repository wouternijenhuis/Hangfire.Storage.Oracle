using System;
using System.Collections.Generic;
using System.Threading;
using Dapper;
using Hangfire.Logging;
using Hangfire.Server;

namespace Hangfire.Oracle.Core.BackgroundProcesses;

/// <summary>
/// Background process that removes expired records from all Hangfire tables.
/// Prevents unbounded database growth by cleaning up completed and expired jobs.
/// </summary>
#pragma warning disable CS0618 // IServerComponent is obsolete but still required
internal sealed class ExpiredRecordsCleanupProcess : IServerComponent
#pragma warning restore CS0618
{
    private static readonly ILog Logger = LogProvider.GetLogger(typeof(ExpiredRecordsCleanupProcess));

    private readonly OracleStorage _storage;
    private readonly TimeSpan _cleanupInterval;
    private readonly int _batchSize;
    private readonly TimeSpan _lockTimeout;

    // Tables to clean in dependency order (children before parents)
    private readonly List<CleanupTarget> _cleanupTargets;

    /// <summary>
    /// Creates a new expired records cleanup process.
    /// </summary>
    /// <param name="storage">The Oracle storage instance.</param>
    /// <param name="cleanupInterval">How often to run cleanup.</param>
    /// <param name="batchSize">Maximum records to delete per batch.</param>
    public ExpiredRecordsCleanupProcess(
        OracleStorage storage,
        TimeSpan cleanupInterval,
        int batchSize = 1000)
    {
        _storage = storage ?? throw new ArgumentNullException(nameof(storage));
        _cleanupInterval = cleanupInterval;
        _batchSize = batchSize;
        _lockTimeout = TimeSpan.FromSeconds(30);

        // Initialize cleanup targets - order matters for FK constraints
        _cleanupTargets = new List<CleanupTarget>
        {
            // Job-related tables (reference JOB table)
            new("JOB_PARAMETER", "JOB_ID", "SELECT ID FROM {JOB} WHERE EXPIRE_AT < :expireAt"),
            new("JOB_QUEUE", "JOB_ID", "SELECT ID FROM {JOB} WHERE EXPIRE_AT < :expireAt"),
            new("JOB_STATE", "JOB_ID", "SELECT ID FROM {JOB} WHERE EXPIRE_AT < :expireAt"),
            
            // Independent tables with their own EXPIRE_AT
            new("AGGREGATED_COUNTER", hasOwnExpiration: true),
            new("LIST", hasOwnExpiration: true),
            new("SET", hasOwnExpiration: true),
            new("HASH", hasOwnExpiration: true),
            
            // Parent job table - clean last
            new("JOB", hasOwnExpiration: true)
        };
    }

    /// <summary>
    /// Executes the cleanup process.
    /// </summary>
    public void Execute(CancellationToken cancellationToken)
    {
        Logger.Debug("Starting expired records cleanup...");

        var currentTime = DateTime.UtcNow;
        var totalDeleted = 0;

        // Acquire distributed lock to prevent concurrent cleanup
        try
        {
            using var distributedLock = new OracleDistributedLock(
                _storage,
                "ExpiredRecordsCleanup",
                _lockTimeout);

            foreach (var target in _cleanupTargets)
            {
                if (cancellationToken.IsCancellationRequested)
                    break;

                var deleted = CleanupTable(target, currentTime, cancellationToken);
                totalDeleted += deleted;
            }

            if (totalDeleted > 0)
            {
                Logger.InfoFormat("Cleaned up {0} expired records.", totalDeleted);
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            Logger.WarnException("Failed to acquire cleanup lock or error during cleanup.", ex);
        }

        // Wait for next cleanup cycle
        cancellationToken.WaitHandle.WaitOne(_cleanupInterval);
    }

    private int CleanupTable(CleanupTarget target, DateTime expireAt, CancellationToken cancellationToken)
    {
        var tableName = _storage.GetTableName(target.TableName);
        var totalDeleted = 0;
        int batchDeleted;

        do
        {
            if (cancellationToken.IsCancellationRequested)
                break;

            batchDeleted = DeleteBatch(target, tableName, expireAt);
            totalDeleted += batchDeleted;

            if (batchDeleted >= _batchSize)
            {
                // Short pause between batches to reduce lock contention
                cancellationToken.WaitHandle.WaitOne(TimeSpan.FromSeconds(1));
            }
        }
        while (batchDeleted >= _batchSize);

        if (totalDeleted > 0)
        {
            Logger.DebugFormat("Deleted {0} expired records from {1}.", totalDeleted, target.TableName);
        }

        return totalDeleted;
    }

    private int DeleteBatch(CleanupTarget target, string tableName, DateTime expireAt)
    {
        try
        {
            using var connection = _storage.CreateAndOpenConnection();

            string deleteSql;

            if (target.HasOwnExpiration)
            {
                // Table has its own EXPIRE_AT column
                deleteSql = $@"
                    DELETE FROM {tableName}
                    WHERE EXPIRE_AT IS NOT NULL 
                      AND EXPIRE_AT < :expireAt
                      AND ROWNUM <= :batchSize";
            }
            else
            {
                // Table references expired jobs via foreign key
                var jobTable = _storage.GetTableName("JOB");
                var parentQuery = target.ParentQuery!.Replace("{JOB}", jobTable);

                deleteSql = $@"
                    DELETE FROM {tableName}
                    WHERE {target.ForeignKeyColumn} IN ({parentQuery})
                      AND ROWNUM <= :batchSize";
            }

            return connection.Execute(deleteSql, new { expireAt, batchSize = _batchSize });
        }
        catch (Exception ex)
        {
            Logger.WarnException($"Error cleaning up table {target.TableName}.", ex);
            return 0;
        }
    }

    /// <inheritdoc />
    public override string ToString() => nameof(ExpiredRecordsCleanupProcess);

    /// <summary>
    /// Defines a table to clean up.
    /// </summary>
    private sealed class CleanupTarget
    {
        public string TableName { get; }
        public string? ForeignKeyColumn { get; }
        public string? ParentQuery { get; }
        public bool HasOwnExpiration { get; }

        /// <summary>
        /// Creates a cleanup target for a table with its own EXPIRE_AT column.
        /// </summary>
        public CleanupTarget(string tableName, bool hasOwnExpiration = false)
        {
            TableName = tableName;
            HasOwnExpiration = hasOwnExpiration;
        }

        /// <summary>
        /// Creates a cleanup target for a table that references another table.
        /// </summary>
        public CleanupTarget(string tableName, string foreignKeyColumn, string parentQuery)
        {
            TableName = tableName;
            ForeignKeyColumn = foreignKeyColumn;
            ParentQuery = parentQuery;
            HasOwnExpiration = false;
        }
    }
}
