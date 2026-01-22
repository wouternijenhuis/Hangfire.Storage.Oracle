using Dapper;

namespace Hangfire.Oracle.Core.Queue;

/// <summary>
/// Oracle-specific queue monitoring implementation.
/// Provides visibility into queue status and job counts.
/// </summary>
internal sealed class OracleQueueMonitor : IQueueMonitor
{
    private readonly OracleStorage _storage;

    public OracleQueueMonitor(OracleStorage storage)
    {
        _storage = storage ?? throw new ArgumentNullException(nameof(storage));
    }

    /// <inheritdoc />
    public IReadOnlyList<string> GetAllQueues()
    {
        using var connection = _storage.CreateAndOpenConnection();
        var queueTableName = _storage.GetTableName("JOB_QUEUE");

        var sql = $@"
            SELECT DISTINCT QUEUE
            FROM {queueTableName}
            ORDER BY QUEUE";

        return connection.Query<string>(sql).ToList();
    }

    /// <inheritdoc />
    public QueueStatistics GetStatistics(string queue)
    {
        if (string.IsNullOrEmpty(queue))
        {
            throw new ArgumentNullException(nameof(queue));
        }

        using var connection = _storage.CreateAndOpenConnection();
        var queueTableName = _storage.GetTableName("JOB_QUEUE");

        var sql = $@"
            SELECT 
                SUM(CASE WHEN FETCHED_AT IS NULL THEN 1 ELSE 0 END) AS EnqueuedCount,
                SUM(CASE WHEN FETCHED_AT IS NOT NULL THEN 1 ELSE 0 END) AS FetchedCount
            FROM {queueTableName}
            WHERE QUEUE = :queue";

        var result = connection.QueryFirstOrDefault<dynamic>(sql, new { queue });

        return new QueueStatistics(
            Convert.ToInt32(result?.ENQUEUEDCOUNT ?? 0),
            Convert.ToInt32(result?.FETCHEDCOUNT ?? 0));
    }

    /// <inheritdoc />
    public IReadOnlyList<long> GetEnqueuedJobIds(string queue, int offset, int limit)
    {
        if (string.IsNullOrEmpty(queue))
        {
            throw new ArgumentNullException(nameof(queue));
        }

        using var connection = _storage.CreateAndOpenConnection();
        var queueTableName = _storage.GetTableName("JOB_QUEUE");

        // Oracle 12c+ syntax with OFFSET/FETCH
        var sql = $@"
            SELECT JOB_ID
            FROM {queueTableName}
            WHERE QUEUE = :queue AND FETCHED_AT IS NULL
            ORDER BY ID
            OFFSET :offset ROWS FETCH NEXT :limit ROWS ONLY";

        return connection.Query<long>(sql, new { queue, offset, limit }).ToList();
    }

    /// <inheritdoc />
    public IReadOnlyList<long> GetFetchedJobIds(string queue, int offset, int limit)
    {
        if (string.IsNullOrEmpty(queue))
        {
            throw new ArgumentNullException(nameof(queue));
        }

        using var connection = _storage.CreateAndOpenConnection();
        var queueTableName = _storage.GetTableName("JOB_QUEUE");

        var sql = $@"
            SELECT JOB_ID
            FROM {queueTableName}
            WHERE QUEUE = :queue AND FETCHED_AT IS NOT NULL
            ORDER BY FETCHED_AT
            OFFSET :offset ROWS FETCH NEXT :limit ROWS ONLY";

        return connection.Query<long>(sql, new { queue, offset, limit }).ToList();
    }
}
