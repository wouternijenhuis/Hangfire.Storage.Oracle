using System;

namespace Hangfire.Oracle.Core.Models;

/// <summary>
/// Represents a set entry stored in the database.
/// </summary>
/// <param name="Id">The unique identifier of the set entry.</param>
/// <param name="Key">The set key.</param>
/// <param name="Value">The set entry value.</param>
/// <param name="Score">The score used for ordering within the set.</param>
/// <param name="ExpireAt">When this set entry should be deleted.</param>
public sealed record StoredSet(
    long Id,
    string Key,
    string Value,
    double Score,
    DateTime? ExpireAt);
