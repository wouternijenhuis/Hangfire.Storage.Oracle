using Hangfire.Logging;
using Hangfire.Storage;
using Oracle.ManagedDataAccess.Client;

namespace Hangfire.Oracle.Core.Queue;

/// <summary>
/// Represents a job that has been fetched from the queue and is being processed.
/// Manages the lifecycle of the fetched job including completion and requeuing.
/// </summary>
internal sealed class FetchedJobContext : IFetchedJob
{
    private static readonly ILog _logger = LogProvider.GetLogger(typeof(FetchedJobContext));
    private readonly OracleStorage _storage;
    private readonly long _queueId;
    private readonly string _fetchToken;
    private readonly Timer _heartbeat;
    private readonly object _syncRoot = new();
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
        var interval = storage.Options.SlidingInvisibilityTimeout;
        _heartbeat = new Timer(ExtendInvisibility, null, interval, interval);
    }

    /// <summary>
    /// Removes the job from the queue after successful processing.
    /// </summary>
    public void RemoveFromQueue()
    {
        lock (_syncRoot)
        {
            ThrowIfDisposed();

            using var connection = _storage.CreateAndOpenConnection();
            var queueTableName = _storage.GetTableName("JOB_QUEUE");

            var deleteSql = $@"
                DELETE FROM {queueTableName}
                WHERE ID = :queueId AND FETCH_TOKEN = :fetchToken";

            var affected = connection.ExecuteNonQuery(deleteSql, _storage.Options.CommandTimeout, cmd =>
            {
                cmd.Parameters.Add(new OracleParameter("queueId", _queueId));
                cmd.Parameters.Add(new OracleParameter("fetchToken", _fetchToken));
            });

            _removedFromQueue = affected == 1;
        }
    }

    /// <summary>
    /// Returns the job to the queue for another worker to process.
    /// </summary>
    public void Requeue()
    {
        lock (_syncRoot)
        {
            ThrowIfDisposed();

            _requeued = RequeueCore();
        }
    }

    private bool RequeueCore()
    {
        using var connection = _storage.CreateAndOpenConnection();
        var queueTableName = _storage.GetTableName("JOB_QUEUE");
        var updateSql = $@"
            UPDATE {queueTableName}
            SET FETCHED_AT = NULL, FETCH_TOKEN = NULL
            WHERE ID = :queueId AND FETCH_TOKEN = :fetchToken";

        return connection.ExecuteNonQuery(updateSql, _storage.Options.CommandTimeout, cmd =>
        {
            cmd.Parameters.Add(new OracleParameter("queueId", _queueId));
            cmd.Parameters.Add(new OracleParameter("fetchToken", _fetchToken));
        }) == 1;
    }

    /// <summary>
    /// Disposes the fetched job context. If the job hasn't been explicitly
    /// removed or requeued, it will be automatically requeued.
    /// </summary>
    public void Dispose()
    {
        lock (_syncRoot)
        {
            if (_disposed)
            {
                return;
            }

            _heartbeat.Dispose();

            if (!_removedFromQueue && !_requeued)
            {
                try
                {
                    _requeued = RequeueCore();
                }
                catch (OracleException ex)
                {
                    _logger.WarnFormat("Could not requeue job {0} during disposal: {1}", JobId, ex.Message);
                }
            }

            _disposed = true;
        }
    }

    private void ExtendInvisibility(object? state)
    {
        lock (_syncRoot)
        {
            if (_disposed || _removedFromQueue || _requeued)
            {
                return;
            }

            try
            {
                using var connection = _storage.CreateAndOpenConnection();
                connection.ExecuteNonQuery(
                    $@"UPDATE {_storage.GetTableName("JOB_QUEUE")}
                       SET FETCHED_AT = :fetchedAt
                       WHERE ID = :queueId AND FETCH_TOKEN = :fetchToken",
                    _storage.Options.CommandTimeout,
                    command =>
                    {
                        command.Parameters.Add(new OracleParameter("fetchedAt", _storage.GetUtcOrLocalNow()));
                        command.Parameters.Add(new OracleParameter("queueId", _queueId));
                        command.Parameters.Add(new OracleParameter("fetchToken", _fetchToken));
                    });
            }
            catch (OracleException ex)
            {
                _logger.WarnFormat("Could not extend invisibility for job {0}: {1}", JobId, ex.Message);
            }
        }
    }

    private void ThrowIfDisposed()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
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
        int commandTimeout,
        Action<OracleCommand>? configure = null)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.CommandTimeout = commandTimeout;
        command.BindByName = true;
        configure?.Invoke(command);
        return command.ExecuteNonQuery();
    }
}
