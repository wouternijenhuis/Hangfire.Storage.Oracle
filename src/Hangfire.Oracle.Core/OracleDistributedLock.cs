using System.Data;
using Dapper;
using Hangfire.Logging;
using Hangfire.Oracle.Core.Exceptions;
using Oracle.ManagedDataAccess.Client;

namespace Hangfire.Oracle.Core;

/// <summary>
/// Provides owner-aware distributed locking using a lease table or Oracle DBMS_LOCK.
/// </summary>
public sealed class OracleDistributedLock : IDisposable
{
    private static readonly ILog _logger = LogProvider.GetLogger(typeof(OracleDistributedLock));
    private readonly OracleStorage _storage;
    private readonly string _resource;
    private readonly string _ownerId = Guid.NewGuid().ToString("N");
    private readonly OracleConnection _connection;
    private readonly Timer? _leaseTimer;
    private string? _dbmsLockHandle;
    private bool _disposed;

    /// <summary>Gets the protected resource name.</summary>
    public string Resource => _resource;

    /// <summary>Gets when this owner acquired the lock.</summary>
    public DateTime AcquiredAt { get; }

    /// <summary>Acquires a distributed lock for the supplied resource.</summary>
    public OracleDistributedLock(OracleStorage storage, string resource, TimeSpan timeout)
    {
        _storage = storage ?? throw new ArgumentNullException(nameof(storage));
        if (string.IsNullOrWhiteSpace(resource))
        {
            throw new ArgumentException("Resource names cannot be empty.", nameof(resource));
        }

        if (timeout < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(timeout));
        }

        _resource = resource;
        _connection = _storage.CreateAndOpenConnection();

        try
        {
            if (_storage.Options.UseDbmsLock)
            {
                AcquireDbmsLock(timeout);
            }
            else
            {
                AcquireTableLock(timeout);
                var renewalInterval = TimeSpan.FromTicks(
                    Math.Max(TimeSpan.FromSeconds(1).Ticks, _storage.Options.DistributedLockTimeout.Ticks / 3));
                _leaseTimer = new Timer(RenewLease, null, renewalInterval, renewalInterval);
            }

            AcquiredAt = _storage.GetUtcOrLocalNow();
        }
        catch (Exception ex)
        {
            _connection.Dispose();
            if (ex is TimeoutException or OracleException)
            {
                throw new DistributedLockAcquisitionException(_resource, timeout, ex);
            }

            throw;
        }
    }

    /// <summary>Renews this owner's lease.</summary>
    public bool Extend()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_storage.Options.UseDbmsLock)
        {
            return _connection.State == ConnectionState.Open && _dbmsLockHandle is not null;
        }

        var now = _storage.GetUtcOrLocalNow();
        return _connection.Execute(
            $@"UPDATE {_storage.GetTableName("DISTRIBUTED_LOCK")}
               SET CREATED_AT = :createdAt, EXPIRE_AT = :expireAt
               WHERE RESOURCE_NAME = :resourceName AND OWNER_ID = :ownerId",
            new
            {
                resourceName = _resource,
                ownerId = _ownerId,
                createdAt = now,
                expireAt = now.Add(_storage.Options.DistributedLockTimeout)
            },
            commandTimeout: _storage.Options.CommandTimeout) == 1;
    }

    /// <summary>Releases the lock only when it is still owned by this instance.</summary>
    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        _leaseTimer?.Dispose();

        try
        {
            if (_storage.Options.UseDbmsLock)
            {
                ReleaseDbmsLock();
            }
            else
            {
                _connection.Execute(
                    $@"DELETE FROM {_storage.GetTableName("DISTRIBUTED_LOCK")}
                       WHERE RESOURCE_NAME = :resourceName AND OWNER_ID = :ownerId",
                    new { resourceName = _resource, ownerId = _ownerId },
                    commandTimeout: _storage.Options.CommandTimeout);
            }
        }
        catch (OracleException ex)
        {
            _logger.WarnException($"Could not release distributed lock '{_resource}'.", ex);
        }
        finally
        {
            _connection.Dispose();
        }
    }

    private void AcquireTableLock(TimeSpan timeout)
    {
        var started = DateTime.UtcNow;
        var attempt = 0;

        while (DateTime.UtcNow - started <= timeout)
        {
            attempt++;
            var now = _storage.GetUtcOrLocalNow();

            try
            {
                var affected = _connection.Execute(
                    $@"MERGE INTO {_storage.GetTableName("DISTRIBUTED_LOCK")} target
                       USING (SELECT :resourceName RESOURCE_NAME FROM DUAL) source
                       ON (target.RESOURCE_NAME = source.RESOURCE_NAME)
                       WHEN MATCHED THEN UPDATE SET
                         target.OWNER_ID = :ownerId,
                         target.CREATED_AT = :createdAt,
                         target.EXPIRE_AT = :expireAt
                         WHERE target.EXPIRE_AT <= :createdAt
                       WHEN NOT MATCHED THEN INSERT (RESOURCE_NAME, OWNER_ID, CREATED_AT, EXPIRE_AT)
                         VALUES (:resourceName, :ownerId, :createdAt, :expireAt)",
                    new
                    {
                        resourceName = _resource,
                        ownerId = _ownerId,
                        createdAt = now,
                        expireAt = now.Add(_storage.Options.DistributedLockTimeout)
                    },
                    commandTimeout: _storage.Options.CommandTimeout);

                if (affected == 1)
                {
                    return;
                }
            }
            catch (OracleException ex) when (
                OracleErrorCodes.IsUniqueConstraintViolation(ex)
                || OracleErrorCodes.IsTransientError(ex.Number))
            {
                _logger.DebugFormat("Lock contention for '{0}' (ORA-{1}).", _resource, ex.Number);
            }

            var remaining = timeout - (DateTime.UtcNow - started);
            if (remaining <= TimeSpan.Zero)
            {
                break;
            }

            var delay = OracleErrorCodes.CalculateRetryDelay(
                Math.Min(attempt, 8),
                TimeSpan.FromMilliseconds(25),
                TimeSpan.FromSeconds(1));
            Thread.Sleep(remaining < delay ? remaining : delay);
        }

        throw new TimeoutException($"Could not acquire distributed lock '{_resource}' within {timeout}.");
    }

    private void AcquireDbmsLock(TimeSpan timeout)
    {
        using var command = _connection.CreateCommand();
        command.BindByName = true;
        command.CommandTimeout = Math.Max(_storage.Options.CommandTimeout, (int)Math.Ceiling(timeout.TotalSeconds) + 5);
        command.CommandText = @"DECLARE
  result NUMBER;
BEGIN
  DBMS_LOCK.ALLOCATE_UNIQUE(:lockName, :lockHandle);
  result := DBMS_LOCK.REQUEST(:lockHandle, DBMS_LOCK.X_MODE, :timeoutSeconds, FALSE);
  :result := result;
END;";
        command.Parameters.Add("lockName", OracleDbType.Varchar2, $"Hangfire:{_resource}", ParameterDirection.Input);
        command.Parameters.Add("lockHandle", OracleDbType.Varchar2, 128, null, ParameterDirection.InputOutput);
        command.Parameters.Add("timeoutSeconds", OracleDbType.Int32, (int)Math.Ceiling(timeout.TotalSeconds), ParameterDirection.Input);
        command.Parameters.Add("result", OracleDbType.Int32, ParameterDirection.Output);
        command.ExecuteNonQuery();

        var result = Convert.ToInt32(command.Parameters["result"].Value.ToString());
        if (result is not (0 or 4))
        {
            throw new TimeoutException($"DBMS_LOCK.REQUEST returned status {result} for '{_resource}'.");
        }

        _dbmsLockHandle = command.Parameters["lockHandle"].Value.ToString();
    }

    private void ReleaseDbmsLock()
    {
        if (_dbmsLockHandle is null)
        {
            return;
        }

        using var command = _connection.CreateCommand();
        command.BindByName = true;
        command.CommandTimeout = _storage.Options.CommandTimeout;
        command.CommandText = "BEGIN :result := DBMS_LOCK.RELEASE(:lockHandle); END;";
        command.Parameters.Add("result", OracleDbType.Int32, ParameterDirection.Output);
        command.Parameters.Add("lockHandle", OracleDbType.Varchar2, _dbmsLockHandle, ParameterDirection.Input);
        command.ExecuteNonQuery();
    }

    private void RenewLease(object? state)
    {
        if (_disposed)
        {
            return;
        }

        try
        {
            if (!Extend())
            {
                _logger.WarnFormat("Distributed lock '{0}' is no longer owned by this instance.", _resource);
            }
        }
        catch (OracleException ex)
        {
            _logger.WarnException($"Could not renew distributed lock '{_resource}'.", ex);
        }
    }
}
