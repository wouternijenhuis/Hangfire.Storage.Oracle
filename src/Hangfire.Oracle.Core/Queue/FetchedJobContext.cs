using System;
using Hangfire.Storage;
using Oracle.ManagedDataAccess.Client;

namespace Hangfire.Oracle.Core.Queue;

/// <summary>
/// Represents a job that has been fetched from the queue and is being processed.
/// Manages the lifecycle of the fetched job including completion and requeuing.
/// </summary>
internal sealed class FetchedJobContext : IFetchedJob
{
    private readonly OracleStorage _storage;
    private readonly long _queueId;
    private readonly string _fetchToken;
    private bool _disposed;
    private bool _removedFromQueue;
    private bool _requeued;

    /// <summary>
    /// Gets the ID of the fetched job.
    /// </summary>
    public string JobId { get; }

    /// <summary>
    /// Gets the name of the queue this job was fetched from.
    /// </summary>
    public string Queue { get; }

    public FetchedJobContext(
        OracleStorage storage,
        long queueId,
        long jobId,
        string queue,
        string fetchToken)
    {
        _storage = storage ?? throw new ArgumentNullException(nameof(storage));
        _queueId = queueId;
        _fetchToken = fetchToken ?? throw new ArgumentNullException(nameof(fetchToken));
        JobId = jobId.ToString();
        Queue = queue ?? throw new ArgumentNullException(nameof(queue));
    }

    /// <summary>
    /// Removes the job from the queue after successful processing.
    /// </summary>
    public void RemoveFromQueue()
    {
        if (_disposed)
            throw new ObjectDisposedException(GetType().Name);

        using var connection = _storage.CreateAndOpenConnection();
        var queueTableName = _storage.GetTableName("JOB_QUEUE");

        var deleteSql = $@"
            DELETE FROM {queueTableName}
            WHERE ID = :queueId AND FETCH_TOKEN = :fetchToken";

        connection.ExecuteNonQuery(deleteSql, cmd =>
        {
            cmd.Parameters.Add(new OracleParameter("queueId", _queueId));
            cmd.Parameters.Add(new OracleParameter("fetchToken", _fetchToken));
        });

        _removedFromQueue = true;
    }

    /// <summary>
    /// Returns the job to the queue for another worker to process.
    /// </summary>
    public void Requeue()
    {
        if (_disposed)
            throw new ObjectDisposedException(GetType().Name);

        using var connection = _storage.CreateAndOpenConnection();
        var queueTableName = _storage.GetTableName("JOB_QUEUE");

        var updateSql = $@"
            UPDATE {queueTableName}
            SET FETCHED_AT = NULL, FETCH_TOKEN = NULL
            WHERE ID = :queueId AND FETCH_TOKEN = :fetchToken";

        connection.ExecuteNonQuery(updateSql, cmd =>
        {
            cmd.Parameters.Add(new OracleParameter("queueId", _queueId));
            cmd.Parameters.Add(new OracleParameter("fetchToken", _fetchToken));
        });

        _requeued = true;
    }

    /// <summary>
    /// Disposes the fetched job context. If the job hasn't been explicitly
    /// removed or requeued, it will be automatically requeued.
    /// </summary>
    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        // If the job wasn't explicitly handled, requeue it
        if (!_removedFromQueue && !_requeued)
        {
            try
            {
                Requeue();
            }
            catch
            {
                // Ignore errors during disposal - the invisibility timeout will handle it
            }
        }
    }
}

/// <summary>
/// Extension methods for OracleConnection to simplify command execution.
/// </summary>
internal static class OracleConnectionExtensions
{
    public static int ExecuteNonQuery(
        this OracleConnection connection,
        string sql,
        Action<OracleCommand>? configure = null)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        configure?.Invoke(command);
        return command.ExecuteNonQuery();
    }
}
