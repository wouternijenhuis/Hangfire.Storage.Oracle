using System;
using System.Data;
using System.Linq;
using System.Threading;
using Dapper;
using Hangfire.Logging;
using Hangfire.Storage;
using Oracle.ManagedDataAccess.Client;

namespace Hangfire.Oracle.Core.Queue;

/// <summary>
/// Oracle-specific job queue implementation using database-level locking.
/// Optimized for Oracle 19c and higher with <c>FOR UPDATE SKIP LOCKED</c> support.
/// </summary>
/// <remarks>
/// <para>
/// This implementation uses Oracle 19c's <c>SKIP LOCKED</c> clause for non-blocking
/// concurrent job dequeuing. Multiple workers can efficiently fetch jobs without
/// blocking each other.
/// </para>
/// <para>
/// For Oracle versions prior to 19c, set <see cref="OracleStorageOptions.UseSkipLocked"/>
/// to <c>false</c> to use alternative locking mechanisms.
/// </para>
/// </remarks>
internal sealed class OracleQueue : IJobQueue
{
    private static readonly ILog Logger = LogProvider.GetLogger(typeof(OracleQueue));

    private readonly OracleStorage _storage;
    private readonly OracleStorageOptions _options;

    public OracleQueue(OracleStorage storage, OracleStorageOptions options)
    {
        _storage = storage ?? throw new ArgumentNullException(nameof(storage));
        _options = options ?? throw new ArgumentNullException(nameof(options));
    }

    /// <inheritdoc />
    public IFetchedJob? Dequeue(string[] queues, CancellationToken cancellationToken)
    {
        if (queues == null || queues.Length == 0)
            throw new ArgumentException("At least one queue name must be specified.", nameof(queues));

        var pollAttempts = 0;

        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var fetchedJob = _options.UseSkipLocked
                    ? TryFetchJobWithSkipLocked(queues)
                    : TryFetchJobClassic(queues);

                if (fetchedJob != null)
                {
                    Logger.TraceFormat("Dequeued job {0} from queue '{1}'.", fetchedJob.JobId, fetchedJob.Queue);
                    return fetchedJob;
                }
            }
            catch (OracleException ex) when (OracleErrorCodes.IsTransientError(ex.Number))
            {
                Logger.WarnFormat("Transient error during dequeue (ORA-{0}), retrying...", ex.Number);
            }

            // Increment poll attempts for exponential backoff on empty queues
            pollAttempts++;

            // Use exponential backoff up to the configured poll interval
            var waitTime = pollAttempts <= 3
                ? TimeSpan.FromMilliseconds(Math.Min(100 * Math.Pow(2, pollAttempts), _options.QueuePollInterval.TotalMilliseconds))
                : _options.QueuePollInterval;

            cancellationToken.WaitHandle.WaitOne(waitTime);
        }

        cancellationToken.ThrowIfCancellationRequested();
        return null;
    }

    /// <summary>
    /// Fetches a job using Oracle 19c+ SKIP LOCKED for non-blocking concurrent access.
    /// </summary>
    private FetchedJobContext? TryFetchJobWithSkipLocked(string[] queues)
    {
        var queueTableName = _storage.GetTableName("JOB_QUEUE");
        var fetchToken = Guid.NewGuid().ToString("N");
        var currentTime = GetCurrentTime();

        using var connection = _storage.CreateAndOpenConnection();
        using var transaction = connection.BeginTransaction();

        try
        {
            // Oracle 19c+: Use FETCH FIRST with FOR UPDATE SKIP LOCKED
            // This is more efficient than ROWNUM for limiting with locks
            var queueParams = BuildQueueParameters(queues);

            var selectForUpdateSql = $@"
                SELECT ID
                FROM {queueTableName}
                WHERE QUEUE IN ({queueParams.ParamList})
                  AND (FETCHED_AT IS NULL OR FETCHED_AT < :invisibilityTimeout)
                ORDER BY ID
                FETCH FIRST 1 ROW ONLY
                FOR UPDATE SKIP LOCKED";

            var parameters = new DynamicParameters();
            parameters.Add("invisibilityTimeout", currentTime.Add(-_options.InvisibilityTimeout));
            foreach (var qp in queueParams.Parameters)
            {
                parameters.Add(qp.Key, qp.Value);
            }

            var lockedId = connection.QueryFirstOrDefault<long?>(
                selectForUpdateSql,
                parameters,
                transaction,
                commandTimeout: _options.CommandTimeout);

            if (!lockedId.HasValue)
            {
                transaction.Rollback();
                return null;
            }

            // Update the locked row
            connection.Execute(
                $@"UPDATE {queueTableName}
                   SET FETCHED_AT = :fetchedAt, FETCH_TOKEN = :fetchToken
                   WHERE ID = :id",
                new { id = lockedId.Value, fetchedAt = currentTime, fetchToken },
                transaction,
                commandTimeout: _options.CommandTimeout);

            // Retrieve job details
            var jobData = connection.QueryFirst<QueuedJobRecord>(
                $@"SELECT ID, JOB_ID, QUEUE FROM {queueTableName} WHERE ID = :id",
                new { id = lockedId.Value },
                transaction,
                commandTimeout: _options.CommandTimeout);

            transaction.Commit();

            return new FetchedJobContext(
                _storage,
                jobData.Id,
                jobData.JobId,
                jobData.Queue,
                fetchToken);
        }
        catch
        {
            try { transaction.Rollback(); } catch { /* ignore */ }
            throw;
        }
    }

    /// <summary>
    /// Fetches a job using classic ROWNUM-based locking (Oracle 12c-18c compatible).
    /// </summary>
    private FetchedJobContext? TryFetchJobClassic(string[] queues)
    {
        var queueTableName = _storage.GetTableName("JOB_QUEUE");
        var fetchToken = Guid.NewGuid().ToString("N");
        var currentTime = GetCurrentTime();

        using var connection = _storage.CreateAndOpenConnection();
        using var transaction = connection.BeginTransaction();

        try
        {
            var queueParams = BuildQueueParameters(queues);

            // Classic approach: subquery with ROWNUM, then FOR UPDATE
            var updateSql = $@"
                UPDATE {queueTableName}
                SET FETCHED_AT = :fetchedAt, FETCH_TOKEN = :fetchToken
                WHERE ID = (
                    SELECT ID FROM (
                        SELECT ID FROM {queueTableName}
                        WHERE QUEUE IN ({queueParams.ParamList})
                          AND (FETCHED_AT IS NULL OR FETCHED_AT < :invisibilityTimeout)
                        ORDER BY ID
                    ) WHERE ROWNUM = 1
                    FOR UPDATE NOWAIT
                )";

            var parameters = new DynamicParameters();
            parameters.Add("fetchedAt", currentTime);
            parameters.Add("fetchToken", fetchToken);
            parameters.Add("invisibilityTimeout", currentTime.Add(-_options.InvisibilityTimeout));
            foreach (var qp in queueParams.Parameters)
            {
                parameters.Add(qp.Key, qp.Value);
            }

            int updated;
            try
            {
                updated = connection.Execute(updateSql, parameters, transaction, _options.CommandTimeout);
            }
            catch (OracleException ex) when (OracleErrorCodes.IsResourceBusy(ex))
            {
                // Row is locked by another session, skip
                transaction.Rollback();
                return null;
            }

            if (updated == 0)
            {
                transaction.Rollback();
                return null;
            }

            var jobData = connection.QueryFirst<QueuedJobRecord>(
                $@"SELECT ID, JOB_ID, QUEUE FROM {queueTableName} WHERE FETCH_TOKEN = :fetchToken",
                new { fetchToken },
                transaction,
                commandTimeout: _options.CommandTimeout);

            transaction.Commit();

            return new FetchedJobContext(
                _storage,
                jobData.Id,
                jobData.JobId,
                jobData.Queue,
                fetchToken);
        }
        catch (OracleException ex) when (OracleErrorCodes.IsResourceBusy(ex))
        {
            try { transaction.Rollback(); } catch { /* ignore */ }
            return null;
        }
        catch
        {
            try { transaction.Rollback(); } catch { /* ignore */ }
            throw;
        }
    }

    /// <inheritdoc />
    public void Enqueue(IDbConnection connection, IDbTransaction? transaction, string queue, string jobId)
    {
        if (string.IsNullOrEmpty(queue))
            throw new ArgumentNullException(nameof(queue));
        if (string.IsNullOrEmpty(jobId))
            throw new ArgumentNullException(nameof(jobId));

        var queueTableName = _storage.GetTableName("JOB_QUEUE");

        // Use sequence for ID if configured, otherwise rely on trigger
        var insertSql = $@"
            INSERT INTO {queueTableName} (ID, JOB_ID, QUEUE, FETCHED_AT)
            VALUES ({_storage.GetTableName("JOB_QUEUE_SEQ")}.NEXTVAL, :jobId, :queue, NULL)";

        connection.Execute(
            insertSql,
            new { jobId = long.Parse(jobId), queue },
            transaction,
            commandTimeout: _options.CommandTimeout);

        Logger.TraceFormat("Enqueued job {0} to queue '{1}'.", jobId, queue);
    }

    private DateTime GetCurrentTime() =>
        _options.UseUtcTime ? DateTime.UtcNow : DateTime.Now;

    private static (string ParamList, Dictionary<string, string> Parameters) BuildQueueParameters(string[] queues)
    {
        var parameters = new Dictionary<string, string>();
        var paramNames = new string[queues.Length];

        for (var i = 0; i < queues.Length; i++)
        {
            var paramName = $"q{i}";
            paramNames[i] = $":{paramName}";
            parameters[paramName] = queues[i];
        }

        return (string.Join(", ", paramNames), parameters);
    }

    /// <summary>
    /// Internal record for mapping queue table results.
    /// </summary>
    private sealed record QueuedJobRecord
    {
        public long Id { get; init; }
        public long JobId { get; init; }
        public string Queue { get; init; } = string.Empty;
    }
}
