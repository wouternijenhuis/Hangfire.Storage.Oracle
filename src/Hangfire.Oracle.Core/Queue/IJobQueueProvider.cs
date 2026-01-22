namespace Hangfire.Oracle.Core.Queue;

/// <summary>
/// Factory interface for creating job queue and monitoring instances.
/// </summary>
public interface IJobQueueProvider
{
    /// <summary>
    /// Gets the job queue implementation for fetching and enqueueing jobs.
    /// </summary>
    /// <returns>An instance of IJobQueue.</returns>
    IJobQueue GetQueue();

    /// <summary>
    /// Gets the queue monitor for observing queue statistics.
    /// </summary>
    /// <returns>An instance of IQueueMonitor.</returns>
    IQueueMonitor GetMonitor();
}
