using System;

namespace Hangfire.Oracle.Core.Models;

/// <summary>
/// Represents a stored job record in the database.
/// </summary>
/// <param name="Id">The unique identifier of the job.</param>
/// <param name="StateName">The current state name of the job.</param>
/// <param name="InvocationData">Serialized data containing the method to invoke.</param>
/// <param name="Arguments">Serialized arguments for the job method.</param>
/// <param name="CreatedAt">When the job was created.</param>
/// <param name="ExpireAt">When the job record should be deleted.</param>
public sealed record StoredJob(
    long Id,
    string? StateName,
    string InvocationData,
    string Arguments,
    DateTime CreatedAt,
    DateTime? ExpireAt);
