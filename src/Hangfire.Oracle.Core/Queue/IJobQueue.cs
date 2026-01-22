using System.Data;
using Hangfire.Storage;

namespace Hangfire.Oracle.Core.Queue;

/// <summary>
/// Interface for job queue operations.
/// Provides methods to fetch jobs from queues and add new jobs to queues.
/// </summary>
public interface IJobQueue
{
    /// <summary>
    /// Attempts to dequeue a job from one of the specified queues.
    /// </summary>
    /// <param name="queues">Array of queue names to check for available jobs.</param>
    /// <param name="cancellationToken">Cancellation token to stop the dequeue operation.</param>
    /// <returns>A fetched job wrapper, or null if no job is available.</returns>
    IFetchedJob? Dequeue(string[] queues, CancellationToken cancellationToken);

    /// <summary>
    /// Adds a job to the specified queue.
    /// </summary>
    /// <param name="connection">Active database connection.</param>
    /// <param name="transaction">Optional transaction for atomic operations.</param>
    /// <param name="queue">Name of the queue.</param>
    /// <param name="jobId">ID of the job to enqueue.</param>
    void Enqueue(IDbConnection connection, IDbTransaction? transaction, string queue, string jobId);
}
