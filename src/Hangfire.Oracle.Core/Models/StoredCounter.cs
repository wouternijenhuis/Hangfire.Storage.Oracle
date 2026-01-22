namespace Hangfire.Oracle.Core.Models;

/// <summary>
/// Represents a counter entry stored in the database.
/// </summary>
/// <param name="Id">The unique identifier of the counter.</param>
/// <param name="Key">The counter key.</param>
/// <param name="Value">The counter value (can be positive or negative).</param>
/// <param name="ExpireAt">When this counter should be deleted.</param>
public sealed record StoredCounter(
    long Id,
    string Key,
    int Value,
    DateTime? ExpireAt);
