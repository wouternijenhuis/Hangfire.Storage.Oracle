using System;
using System.Data;
using System.Linq;
using System.Threading;
using Dapper;
using Hangfire.Storage;
using Oracle.ManagedDataAccess.Client;

namespace Hangfire.Oracle.Core.Queue;

/// <summary>
/// Oracle-specific job queue implementation using database-level locking.
/// </summary>
internal sealed class OracleQueue : IJobQueue
{
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

        while (!cancellationToken.IsCancellationRequested)
        {
            var fetchedJob = TryFetchJob(queues);
            if (fetchedJob != null)
                return fetchedJob;

            // No job available, wait before trying again
            cancellationToken.WaitHandle.WaitOne(_options.QueuePollInterval);
        }

        cancellationToken.ThrowIfCancellationRequested();
        return null;
    }

    private FetchedJobContext? TryFetchJob(string[] queues)
    {
        var queueTableName = _storage.GetTableName("JOB_QUEUE");
        var fetchToken = Guid.NewGuid().ToString("N");
        var currentTime = _options.UseUtcTime ? DateTime.UtcNow : DateTime.Now;

        using var connection = _storage.CreateAndOpenConnection();
        using var transaction = connection.BeginTransaction();

        try
        {
            // Build queue parameter list for the IN clause
            var queueParams = queues.Select((q, i) => $":q{i}").ToArray();
            var queueParamList = string.Join(", ", queueParams);

            // Update a single job atomically using ROWNUM and FOR UPDATE SKIP LOCKED
            var updateSql = $@"
                UPDATE {queueTableName}
                SET FETCHED_AT = :fetchedAt, FETCH_TOKEN = :fetchToken
                WHERE ID = (
                    SELECT ID FROM {queueTableName}
                    WHERE QUEUE IN ({queueParamList})
                      AND (FETCHED_AT IS NULL OR FETCHED_AT < :invisibilityTimeout)
                      AND ROWNUM = 1
                    FOR UPDATE SKIP LOCKED
                )";

            var parameters = new DynamicParameters();
            parameters.Add("fetchedAt", currentTime);
            parameters.Add("fetchToken", fetchToken);
            parameters.Add("invisibilityTimeout", currentTime.Add(-_options.InvisibilityTimeout));

            for (int i = 0; i < queues.Length; i++)
            {
                parameters.Add($"q{i}", queues[i]);
            }

            var updated = connection.Execute(updateSql, parameters, transaction);

            if (updated == 0)
            {
                transaction.Rollback();
                return null;
            }

            // Retrieve the job we just claimed
            var selectSql = $@"
                SELECT ID, JOB_ID, QUEUE
                FROM {queueTableName}
                WHERE FETCH_TOKEN = :fetchToken";

            var jobData = connection.QueryFirstOrDefault<dynamic>(
                selectSql,
                new { fetchToken },
                transaction);

            if (jobData == null)
            {
                transaction.Rollback();
                return null;
            }

            transaction.Commit();

            return new FetchedJobContext(
                _storage,
                Convert.ToInt64(jobData.ID),
                Convert.ToInt64(jobData.JOB_ID),
                (string)jobData.QUEUE,
                fetchToken);
        }
        catch
        {
            transaction.Rollback();
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
        var insertSql = $@"
            INSERT INTO {queueTableName} (JOB_ID, QUEUE)
            VALUES (:jobId, :queue)";

        connection.Execute(insertSql, new { jobId = long.Parse(jobId), queue }, transaction);
    }
}
