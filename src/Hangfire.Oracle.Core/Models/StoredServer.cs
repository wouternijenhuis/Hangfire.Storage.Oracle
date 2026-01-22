using System;

namespace Hangfire.Oracle.Core.Models;

/// <summary>
/// Represents a Hangfire server registration in the database.
/// </summary>
/// <param name="Id">The unique server identifier.</param>
/// <param name="Data">Serialized server metadata (queues, worker count, etc.).</param>
/// <param name="LastHeartbeat">The last time this server sent a heartbeat.</param>
public sealed record StoredServer(
    string Id,
    string Data,
    DateTime LastHeartbeat);
