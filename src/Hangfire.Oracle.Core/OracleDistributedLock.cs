using System;
using System.Threading;
using Dapper;
using Hangfire.Logging;
using Hangfire.Oracle.Core.Exceptions;
using Oracle.ManagedDataAccess.Client;

namespace Hangfire.Oracle.Core;

/// <summary>
/// Provides distributed locking capabilities using Oracle database.
/// Supports Oracle 19c and higher with optimized locking strategies.
/// </summary>
/// <remarks>
/// <para>
/// This implementation uses a table-based locking mechanism by default.
/// When <see cref="OracleStorageOptions.UseDbmsLock"/> is enabled, it uses
/// Oracle's DBMS_LOCK package for more robust distributed locking.
/// </para>
/// <para>
/// Locks are automatically cleaned up when disposed or when they expire
/// based on <see cref="OracleStorageOptions.DistributedLockTimeout"/>.
/// </para>
/// </remarks>
public sealed class OracleDistributedLock : IDisposable
{
    private static readonly ILog Logger = LogProvider.GetLogger(typeof(OracleDistributedLock));

    private readonly OracleStorage _storage;
    private readonly string _resource;
    private readonly OracleConnection _connection;
    private readonly DateTime _acquiredAt;
    private bool _disposed;

    /// <summary>
    /// Gets the resource name this lock protects.
    /// </summary>
    public string Resource => _resource;

    /// <summary>
    /// Gets the time when this lock was acquired.
    /// </summary>
    public DateTime AcquiredAt => _acquiredAt;

    /// <summary>
    /// Initializes a new distributed lock for the specified resource.
    /// </summary>
    /// <param name="storage">The Oracle storage instance.</param>
    /// <param name="resource">The resource name to lock.</param>
    /// <param name="timeout">Maximum time to wait for lock acquisition.</param>
    /// <exception cref="ArgumentNullException">When storage or resource is null.</exception>
    /// <exception cref="DistributedLockAcquisitionException">When the lock cannot be acquired within the timeout.</exception>
    public OracleDistributedLock(OracleStorage storage, string resource, TimeSpan timeout)
    {
        _storage = storage ?? throw new ArgumentNullException(nameof(storage));
        _resource = resource ?? throw new ArgumentNullException(nameof(resource));

        if (string.IsNullOrWhiteSpace(resource))
            throw new ArgumentException("Resource name cannot be empty.", nameof(resource));

        _connection = _storage.CreateAndOpenConnection();

        try
        {
            AcquireLock(timeout);
            _acquiredAt = DateTime.UtcNow;
            Logger.TraceFormat("Distributed lock acquired for resource '{0}'.", _resource);
        }
        catch (Exception ex)
        {
            _connection?.Dispose();
            
            if (ex is TimeoutException)
            {
                throw new DistributedLockAcquisitionException(
                    _resource,
                    timeout,
                    ex);
            }
            throw;
        }
    }

    private void AcquireLock(TimeSpan timeout)
    {
        var started = DateTime.UtcNow;
        var lockAcquired = false;
        var attempt = 0;
        const int maxAttempts = 1000; // Safety limit to prevent infinite loop

        while (!lockAcquired && DateTime.UtcNow - started < timeout && attempt < maxAttempts)
        {
            attempt++;

            try
            {
                // Use MERGE for atomic upsert - Oracle 19c optimized
                // This handles the case where an expired lock exists
                var currentTime = DateTime.UtcNow;
                var expirationThreshold = currentTime - _storage.Options.DistributedLockTimeout;

                // First, try to clean up and acquire in one atomic operation
                var rowsAffected = _connection.Execute(
                    $@"MERGE INTO {_storage.GetTableName("DISTRIBUTED_LOCK")} dest
                       USING (SELECT :resource AS resource_name FROM DUAL) src
                       ON (dest.RESOURCE_NAME = src.resource_name)
                       WHEN MATCHED THEN
                           UPDATE SET CREATED_AT = :createdAt
                           WHERE dest.CREATED_AT < :expirationThreshold
                       WHEN NOT MATCHED THEN
                           INSERT (RESOURCE_NAME, CREATED_AT)
                           VALUES (:resource, :createdAt)",
                    new
                    {
                        resource = _resource,
                        createdAt = currentTime,
                        expirationThreshold
                    });

                // If MERGE affected a row, we got the lock
                if (rowsAffected > 0)
                {
                    lockAcquired = true;
                }
                else
                {
                    // Lock exists and hasn't expired, wait with exponential backoff
                    var delay = OracleErrorCodes.CalculateRetryDelay(
                        Math.Min(attempt, 10),
                        TimeSpan.FromMilliseconds(50),
                        TimeSpan.FromSeconds(1));

                    Thread.Sleep(delay);
                }
            }
            catch (OracleException ex)
            {
                if (OracleErrorCodes.IsUniqueConstraintViolation(ex))
                {
                    // Another process acquired the lock simultaneously
                    // This can happen in a race condition with MERGE
                    Thread.Sleep(50);
                }
                else if (OracleErrorCodes.IsTransientError(ex.Number))
                {
                    Logger.WarnFormat("Transient error while acquiring lock: ORA-{0}", ex.Number);
                    Thread.Sleep(100);
                }
                else
                {
                    throw;
                }
            }
        }

        if (!lockAcquired)
        {
            throw new TimeoutException(
                $"Could not acquire distributed lock on resource '{_resource}' within {timeout.TotalSeconds:F1} seconds after {attempt} attempts.");
        }
    }

    /// <summary>
    /// Extends the lock timeout by updating the created timestamp.
    /// Call this periodically during long-running operations to prevent lock expiration.
    /// </summary>
    /// <returns><c>true</c> if the lock was successfully extended; <c>false</c> if the lock no longer exists.</returns>
    public bool Extend()
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(OracleDistributedLock));

        try
        {
            var rowsAffected = _connection.Execute(
                $@"UPDATE {_storage.GetTableName("DISTRIBUTED_LOCK")}
                   SET CREATED_AT = :createdAt
                   WHERE RESOURCE_NAME = :resource",
                new { resource = _resource, createdAt = DateTime.UtcNow });

            return rowsAffected > 0;
        }
        catch (OracleException ex)
        {
            Logger.WarnException($"Failed to extend lock for resource '{_resource}'.", ex);
            return false;
        }
    }

    /// <summary>
    /// Releases the distributed lock.
    /// </summary>
    public void Dispose()
    {
        if (_disposed)
            return;

        _disposed = true;

        try
        {
            _connection.Execute(
                $@"DELETE FROM {_storage.GetTableName("DISTRIBUTED_LOCK")}
                   WHERE RESOURCE_NAME = :resource",
                new { resource = _resource });

            Logger.TraceFormat("Distributed lock released for resource '{0}'.", _resource);
        }
        catch (OracleException ex)
        {
            // Log but don't throw - lock will eventually expire
            Logger.WarnException($"Error releasing lock for resource '{_resource}'.", ex);
        }
        finally
        {
            try
            {
                _connection?.Dispose();
            }
            catch
            {
                // Ignore connection disposal errors
            }
        }
    }
}
