namespace Hangfire.Oracle.Core.Models;

/// <summary>
/// Represents a hash entry stored in the database.
/// </summary>
/// <param name="Id">The unique identifier of the hash entry.</param>
/// <param name="Key">The hash key (groups related fields together).</param>
/// <param name="Field">The field name within the hash.</param>
/// <param name="Value">The field value.</param>
/// <param name="ExpireAt">When this hash entry should be deleted.</param>
public sealed record StoredHash(
    long Id,
    string Key,
    string Field,
    string? Value,
    DateTime? ExpireAt);
