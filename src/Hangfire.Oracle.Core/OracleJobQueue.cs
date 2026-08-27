using Hangfire.Oracle.Core.Queue;
using Hangfire.Storage;

namespace Hangfire.Oracle.Core;

/// <summary>
/// Provides backward-compatible access to the Oracle job queue.
/// </summary>
public sealed class OracleJobQueue
{
    private readonly OracleQueue _queue;
    private readonly string _queueName;

    /// <summary>
    /// Initializes a queue for the specified storage and queue name.
    /// </summary>
    public OracleJobQueue(OracleStorage storage, string queue)
    {
        ArgumentNullException.ThrowIfNull(storage);
        if (string.IsNullOrWhiteSpace(queue))
        {
            throw new ArgumentException("Queue names cannot be empty.", nameof(queue));
        }

        _queueName = queue;
        _queue = new OracleQueue(storage, storage.Options);
    }

    /// <summary>
    /// Attempts to fetch one job. Returns <see langword="null"/> when the queue is empty.
    /// </summary>
    public IFetchedJob? Dequeue(CancellationToken cancellationToken)
    {
        return _queue.Dequeue(new[] { _queueName }, cancellationToken);
    }
}
