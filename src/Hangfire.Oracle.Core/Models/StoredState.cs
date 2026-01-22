namespace Hangfire.Oracle.Core.Models;

/// <summary>
/// Represents a job state history record in the database.
/// </summary>
/// <param name="Id">The unique identifier of the state record.</param>
/// <param name="JobId">The job this state belongs to.</param>
/// <param name="Name">The name of the state (e.g., Enqueued, Processing, Succeeded).</param>
/// <param name="Reason">Optional reason for the state transition.</param>
/// <param name="Data">Serialized state-specific data.</param>
/// <param name="CreatedAt">When this state was created.</param>
public sealed record StoredState(
    long Id,
    long JobId,
    string Name,
    string? Reason,
    string? Data,
    DateTime CreatedAt);
