using System;
using System.Threading;
using Dapper;
using Oracle.ManagedDataAccess.Client;

namespace Hangfire.Oracle.Core;

/// <summary>
/// Oracle distributed lock implementation
/// </summary>
public class OracleDistributedLock : IDisposable
{
    private readonly OracleStorage _storage;
    private readonly string _resource;
    private readonly OracleConnection _connection;
    private bool _disposed;

    public OracleDistributedLock(OracleStorage storage, string resource, TimeSpan timeout)
    {
        _storage = storage ?? throw new ArgumentNullException(nameof(storage));
        _resource = resource ?? throw new ArgumentNullException(nameof(resource));

        _connection = _storage.CreateAndOpenConnection();

        try
        {
            AcquireLock(timeout);
        }
        catch
        {
            _connection?.Dispose();
            throw;
        }
    }

    private void AcquireLock(TimeSpan timeout)
    {
        var started = DateTime.UtcNow;
        var lockAcquired = false;

        while (!lockAcquired && DateTime.UtcNow - started < timeout)
        {
            try
            {
                _connection.Execute(
                    $@"INSERT INTO {_storage.GetTableName("DISTRIBUTED_LOCK")} (RESOURCE, CREATED_AT)
                       VALUES (:resource, :createdAt)",
                    new { resource = _resource, createdAt = DateTime.UtcNow });

                lockAcquired = true;
            }
            catch (OracleException ex)
            {
                // Unique constraint violation means lock already exists
                if (ex.Number == 1) // ORA-00001: unique constraint violated
                {
                    // Clean up expired locks
                    CleanupExpiredLocks();

                    // Wait before retry
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
                $"Could not acquire distributed lock on resource '{_resource}' within {timeout.TotalSeconds} seconds.");
        }
    }

    private void CleanupExpiredLocks()
    {
        var timeout = _storage.Options.DistributedLockTimeout;
        var expirationTime = DateTime.UtcNow - timeout;

        _connection.Execute(
            $@"DELETE FROM {_storage.GetTableName("DISTRIBUTED_LOCK")}
               WHERE RESOURCE = :resource AND CREATED_AT < :expirationTime",
            new { resource = _resource, expirationTime });
    }

    public void Dispose()
    {
        if (_disposed)
            return;

        _disposed = true;

        try
        {
            _connection.Execute(
                $@"DELETE FROM {_storage.GetTableName("DISTRIBUTED_LOCK")}
                   WHERE RESOURCE = :resource",
                new { resource = _resource });
        }
        catch
        {
            // Ignore errors during cleanup
        }
        finally
        {
            _connection?.Dispose();
        }
    }
}
