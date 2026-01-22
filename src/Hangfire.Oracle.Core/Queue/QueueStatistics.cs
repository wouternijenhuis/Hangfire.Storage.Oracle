namespace Hangfire.Oracle.Core.Queue;

/// <summary>
/// Statistics for a job queue.
/// </summary>
/// <param name="EnqueuedCount">Number of jobs waiting to be processed.</param>
/// <param name="FetchedCount">Number of jobs currently being processed.</param>
public sealed record QueueStatistics(int EnqueuedCount, int FetchedCount);
