namespace Hangfire.Oracle.Core.Queue;

/// <summary>
/// Interface for monitoring job queue status and statistics.
/// </summary>
public interface IQueueMonitor
{
    /// <summary>
    /// Gets all queue names that have been used.
    /// </summary>
    /// <returns>A list of queue names.</returns>
    IReadOnlyList<string> GetAllQueues();

    /// <summary>
    /// Gets statistics for a specific queue.
    /// </summary>
    /// <param name="queue">The queue name.</param>
    /// <returns>Statistics containing counts of enqueued and fetched jobs.</returns>
    QueueStatistics GetStatistics(string queue);

    /// <summary>
    /// Gets job IDs that are waiting in the queue (not yet fetched).
    /// </summary>
    /// <param name="queue">The queue name.</param>
    /// <param name="offset">Number of jobs to skip.</param>
    /// <param name="limit">Maximum number of job IDs to return.</param>
    /// <returns>A list of job IDs.</returns>
    IReadOnlyList<long> GetEnqueuedJobIds(string queue, int offset, int limit);

    /// <summary>
    /// Gets job IDs that have been fetched but not yet completed.
    /// </summary>
    /// <param name="queue">The queue name.</param>
    /// <param name="offset">Number of jobs to skip.</param>
    /// <param name="limit">Maximum number of job IDs to return.</param>
    /// <returns>A list of job IDs.</returns>
    IReadOnlyList<long> GetFetchedJobIds(string queue, int offset, int limit);
}
