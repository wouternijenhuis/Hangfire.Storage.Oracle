namespace Hangfire.Oracle.Core.Models;

/// <summary>
/// Represents a job parameter stored in the database.
/// </summary>
/// <param name="Id">The unique identifier of the parameter record.</param>
/// <param name="JobId">The job this parameter belongs to.</param>
/// <param name="Name">The parameter name.</param>
/// <param name="Value">The parameter value.</param>
public sealed record StoredJobParameter(
    long Id,
    long JobId,
    string Name,
    string? Value);
