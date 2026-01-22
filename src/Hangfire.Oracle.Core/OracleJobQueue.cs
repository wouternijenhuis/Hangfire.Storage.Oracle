using System;
using System.Threading;
using Dapper;
using Hangfire.Storage;
using Oracle.ManagedDataAccess.Client;

namespace Hangfire.Oracle.Core;

/// <summary>
/// Oracle job queue implementation for fetching jobs from queues
/// </summary>
public class OracleJobQueue
{
    private readonly OracleStorage _storage;
    private readonly string _queue;

    public OracleJobQueue(OracleStorage storage, string queue)
    {
        _storage = storage ?? throw new ArgumentNullException(nameof(storage));
        _queue = queue ?? throw new ArgumentNullException(nameof(queue));
    }

    public IFetchedJob? Dequeue(CancellationToken cancellationToken)
    {
        using var connection = _storage.CreateAndOpenConnection();
        using var transaction = connection.BeginTransaction();

        try
        {
            var fetchedAt = DateTime.UtcNow;
            
            // Try to fetch a job from the queue using FOR UPDATE SKIP LOCKED
            var jobQueue = connection.Query(
                $@"SELECT ID, JOB_ID
                   FROM {_storage.GetTableName("JOB_QUEUE")}
                   WHERE QUEUE = :queue AND FETCHED_AT IS NULL AND ROWNUM = 1
                   ORDER BY ID
                   FOR UPDATE SKIP LOCKED",
                new { queue = _queue },
                transaction: transaction)
                .SingleOrDefault();

            if (jobQueue == null)
            {
                transaction.Rollback();
                return null;
            }

            // Mark as fetched
            connection.Execute(
                $@"UPDATE {_storage.GetTableName("JOB_QUEUE")}
                   SET FETCHED_AT = :fetchedAt
                   WHERE ID = :id",
                new { id = jobQueue.ID, fetchedAt },
                transaction: transaction);

            transaction.Commit();

            return new OracleFetchedJob(_storage, jobQueue.ID, jobQueue.JOB_ID.ToString(), _queue);
        }
        catch
        {
            transaction.Rollback();
            throw;
        }
    }
}

/// <summary>
/// Represents a fetched job from the Oracle queue
/// </summary>
internal class OracleFetchedJob : IFetchedJob
{
    private readonly OracleStorage _storage;
    private readonly long _id;
    private bool _disposed;
    private bool _removedFromQueue;
    private bool _requeued;

    public OracleFetchedJob(OracleStorage storage, long id, string jobId, string queue)
    {
        _storage = storage ?? throw new ArgumentNullException(nameof(storage));
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
            $@"DELETE FROM {_storage.GetTableName("JOB_QUEUE")}
               WHERE ID = :id",
            new { id = _id });

        _removedFromQueue = true;
    }

    public void Requeue()
    {
        using var connection = _storage.CreateAndOpenConnection();

        connection.Execute(
            $@"UPDATE {_storage.GetTableName("JOB_QUEUE")}
               SET FETCHED_AT = NULL
               WHERE ID = :id",
            new { id = _id });

        _requeued = true;
    }

    public void Dispose()
    {
        if (_disposed)
            return;

        _disposed = true;

        if (!_removedFromQueue && !_requeued)
        {
            Requeue();
        }
    }
}
