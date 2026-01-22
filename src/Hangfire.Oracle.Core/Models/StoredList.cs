using System;

namespace Hangfire.Oracle.Core.Models;

/// <summary>
/// Represents a list entry stored in the database.
/// </summary>
/// <param name="Id">The unique identifier of the list entry.</param>
/// <param name="Key">The list key.</param>
/// <param name="Value">The list entry value.</param>
/// <param name="ExpireAt">When this list entry should be deleted.</param>
public sealed record StoredList(
    long Id,
    string Key,
    string Value,
    DateTime? ExpireAt);
