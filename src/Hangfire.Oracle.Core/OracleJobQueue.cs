using Dapper;
using Hangfire.Logging;
using Hangfire.Storage;

namespace Hangfire.Oracle.Core;

/// <summary>
/// Oracle job queue implementation for fetching jobs from queues.
/// Supports Oracle 11g through 23ai with version-specific optimizations.
/// </summary>
public sealed class OracleJobQueue
{
    private static readonly ILog _logger = LogProvider.GetLogger(typeof(OracleJobQueue));

    private readonly OracleStorage _storage;
    private readonly OracleStorageOptions _options;
    private readonly string _queue;

    /// <summary>
    /// Initializes a new instance of the job queue.
    /// </summary>
    public OracleJobQueue(OracleStorage storage, string queue)
    {
        _storage = storage ?? throw new ArgumentNullException(nameof(storage));
        _queue = queue ?? throw new ArgumentNullException(nameof(queue));
        _options = storage.Options;
    }

    /// <summary>
    /// Attempts to dequeue a job from the queue.
    /// Uses version-appropriate SQL syntax based on Oracle version.
    /// </summary>
    public IFetchedJob? Dequeue(CancellationToken cancellationToken)
    {
        // Choose the appropriate dequeue method based on Oracle version
        return _options.SupportsSkipLocked
            ? DequeueWithSkipLocked(cancellationToken)
            : DequeueClassic(cancellationToken);
    }

    /// <summary>
    /// Dequeues using FOR UPDATE SKIP LOCKED (Oracle 19c+).
    /// Most efficient for high-concurrency scenarios.
    /// </summary>
    private IFetchedJob? DequeueWithSkipLocked(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        using var connection = _storage.CreateAndOpenConnection();
        using var transaction = connection.BeginTransaction();

        try
        {
            var fetchedAt = _options.UseUtcTime ? DateTime.UtcNow : DateTime.Now;
            var queueTable = _storage.GetTableName("JOB_QUEUE");

            // Oracle 19c+ with SKIP LOCKED and FETCH FIRST
            var sql = $@"
                SELECT ID, JOB_ID
                FROM {queueTable}
                WHERE QUEUE = :queue AND FETCHED_AT IS NULL
                ORDER BY ID
                FETCH FIRST 1 ROW ONLY
                FOR UPDATE SKIP LOCKED";

            var jobQueue = connection.Query<QueuedJobRecord>(sql, new { queue = _queue }, transaction, commandTimeout: _options.CommandTimeout)
                .FirstOrDefault();

            if (jobQueue is null)
            {
                transaction.Rollback();
                return null;
            }

            // Mark as fetched
            connection.Execute(
                $"UPDATE {queueTable} SET FETCHED_AT = :fetchedAt WHERE ID = :id",
                new { id = jobQueue.Id, fetchedAt },
                transaction,
                commandTimeout: _options.CommandTimeout);

            transaction.Commit();

            _logger.TraceFormat("Dequeued job {0} from queue '{1}' using SKIP LOCKED", jobQueue.JobId, _queue);

            return new OracleFetchedJob(_storage, jobQueue.Id, jobQueue.JobId.ToString(), _queue);
        }
        catch
        {
            transaction.Rollback();
            throw;
        }
    }

    /// <summary>
    /// Dequeues using classic Oracle locking (Oracle 11g-18c).
    /// Uses FOR UPDATE NOWAIT with retry on lock contention.
    /// </summary>
    private IFetchedJob? DequeueClassic(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        using var connection = _storage.CreateAndOpenConnection();
        using var transaction = connection.BeginTransaction();

        try
        {
            var fetchedAt = _options.UseUtcTime ? DateTime.UtcNow : DateTime.Now;
            var queueTable = _storage.GetTableName("JOB_QUEUE");

            // Build SQL based on Oracle version
            string sql;
            if (_options.SupportsFetchFirst)
            {
                // Oracle 12c+ syntax
                sql = $@"
                    SELECT ID, JOB_ID
                    FROM {queueTable}
                    WHERE QUEUE = :queue AND FETCHED_AT IS NULL
                    ORDER BY ID
                    FETCH FIRST 1 ROW ONLY
                    FOR UPDATE NOWAIT";
            }
            else
            {
                // Oracle 11g syntax using ROWNUM
                sql = $@"
                    SELECT ID, JOB_ID
                    FROM (
                        SELECT ID, JOB_ID
                        FROM {queueTable}
                        WHERE QUEUE = :queue AND FETCHED_AT IS NULL
                        ORDER BY ID
                    )
                    WHERE ROWNUM = 1
                    FOR UPDATE NOWAIT";
            }

            QueuedJobRecord? jobQueue = null;

            try
            {
                jobQueue = connection.Query<QueuedJobRecord>(sql, new { queue = _queue }, transaction, commandTimeout: _options.CommandTimeout)
                    .FirstOrDefault();
            }
            catch (Exception ex) when (OracleErrorCodes.IsResourceBusy(ex))
            {
                // ORA-00054: resource busy - another worker has the row locked
                // This is expected in high-concurrency scenarios without SKIP LOCKED
                transaction.Rollback();
                return null;
            }

            if (jobQueue is null)
            {
                transaction.Rollback();
                return null;
            }

            // Mark as fetched
            connection.Execute(
                $"UPDATE {queueTable} SET FETCHED_AT = :fetchedAt WHERE ID = :id",
                new { id = jobQueue.Id, fetchedAt },
                transaction,
                commandTimeout: _options.CommandTimeout);

            transaction.Commit();

            _logger.TraceFormat("Dequeued job {0} from queue '{1}' using classic locking", jobQueue.JobId, _queue);

            return new OracleFetchedJob(_storage, jobQueue.Id, jobQueue.JobId.ToString(), _queue);
        }
        catch
        {
            transaction.Rollback();
            throw;
        }
    }

    /// <summary>
    /// Record type for mapping queue query results.
    /// </summary>
    private sealed record QueuedJobRecord(long Id, long JobId);
}

/// <summary>
/// Represents a fetched job from the Oracle queue.
/// </summary>
internal sealed class OracleFetchedJob : IFetchedJob
{
    private readonly OracleStorage _storage;
    private readonly OracleStorageOptions _options;
    private readonly long _id;
    private bool _disposed;
    private bool _removedFromQueue;
    private bool _requeued;

    public OracleFetchedJob(OracleStorage storage, long id, string jobId, string queue)
    {
        _storage = storage ?? throw new ArgumentNullException(nameof(storage));
        _options = storage.Options;
        _id = id;
        JobId = jobId ?? throw new ArgumentNullException(nameof(jobId));
        Queue = queue ?? throw new ArgumentNullException(nameof(queue));
    }

    public string JobId { get; }
    public string Queue { get; }

    public void RemoveFromQueue()
    {
        using var connection = _storage.CreateAndOpenConnection();

        connection.Execute(
            $"DELETE FROM {_storage.GetTableName("JOB_QUEUE")} WHERE ID = :id",
            new { id = _id },
            commandTimeout: _options.CommandTimeout);

        _removedFromQueue = true;
    }

    public void Requeue()
    {
        using var connection = _storage.CreateAndOpenConnection();

        connection.Execute(
            $"UPDATE {_storage.GetTableName("JOB_QUEUE")} SET FETCHED_AT = NULL WHERE ID = :id",
            new { id = _id },
            commandTimeout: _options.CommandTimeout);

        _requeued = true;
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;

        if (!_removedFromQueue && !_requeued)
        {
            Requeue();
        }
    }
}
