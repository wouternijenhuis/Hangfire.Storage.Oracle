using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Dapper;
using Hangfire.Common;
using Hangfire.Logging;
using Hangfire.States;
using Hangfire.Storage;
using Oracle.ManagedDataAccess.Client;

namespace Hangfire.Oracle.Core;

/// <summary>
/// Oracle write-only transaction implementation for batching job storage operations.
/// </summary>
/// <remarks>
/// <para>
/// Commands are queued and executed together in a single transaction when Commit is called.
/// This provides atomicity and improved performance for batch operations.
/// </para>
/// <para>
/// This implementation uses MERGE statements for upsert operations which are optimized
/// for Oracle 19c+ with proper handling of deterministic operations.
/// </para>
/// </remarks>
public class OracleWriteOnlyTransaction : JobStorageTransaction
{
    private static readonly ILog Logger = LogProvider.GetLogger(typeof(OracleWriteOnlyTransaction));

    private readonly OracleStorage _storage;
    private readonly Queue<CommandAction> _commandQueue = new();
    private readonly int _commandTimeout;

    /// <summary>
    /// Represents a queued command action.
    /// </summary>
    private readonly struct CommandAction
    {
        public string Description { get; init; }
        public Action<OracleConnection, OracleTransaction> Execute { get; init; }
    }

    /// <summary>
    /// Initializes a new write-only transaction.
    /// </summary>
    /// <param name="storage">The Oracle storage instance.</param>
    public OracleWriteOnlyTransaction(OracleStorage storage)
    {
        _storage = storage ?? throw new ArgumentNullException(nameof(storage));
        _commandTimeout = storage.Options.CommandTimeout;
    }

    /// <summary>
    /// Commits all queued commands in a single transaction.
    /// </summary>
    /// <exception cref="OracleException">When a database error occurs.</exception>
    public override void Commit()
    {
        if (_commandQueue.Count == 0)
            return;

        var commandCount = _commandQueue.Count;

        using var connection = _storage.CreateAndOpenConnection();
        using var transaction = connection.BeginTransaction(_storage.Options.TransactionIsolationLevel);

        try
        {
            foreach (var command in _commandQueue)
            {
                command.Execute(connection, transaction);
            }

            transaction.Commit();

            Logger.TraceFormat("Committed transaction with {0} commands.", commandCount);
        }
        catch (OracleException ex)
        {
            transaction.Rollback();

            if (OracleErrorCodes.IsTransientError(ex.Number))
            {
                Logger.WarnFormat("Transient error during commit (ORA-{0}), transaction rolled back.", ex.Number);
            }

            throw;
        }
        catch
        {
            transaction.Rollback();
            throw;
        }
    }

    /// <summary>
    /// Commits all queued commands asynchronously in a single transaction.
    /// </summary>
    public async Task CommitAsync(CancellationToken cancellationToken = default)
    {
        if (_commandQueue.Count == 0)
            return;

        var commandCount = _commandQueue.Count;

        await using var connection = await _storage.CreateAndOpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        using var transaction = connection.BeginTransaction(_storage.Options.TransactionIsolationLevel);

        try
        {
            foreach (var command in _commandQueue)
            {
                command.Execute(connection, transaction);
            }

            transaction.Commit();

            Logger.TraceFormat("Committed async transaction with {0} commands.", commandCount);
        }
        catch (OracleException ex)
        {
            transaction.Rollback();

            if (OracleErrorCodes.IsTransientError(ex.Number))
            {
                Logger.WarnFormat("Transient error during async commit (ORA-{0}), transaction rolled back.", ex.Number);
            }

            throw;
        }
        catch
        {
            transaction.Rollback();
            throw;
        }
    }

    /// <inheritdoc />
    public override void ExpireJob(string jobId, TimeSpan expireIn)
    {
        QueueCommand("ExpireJob", (connection, transaction) =>
        {
            connection.Execute(
                $@"UPDATE {_storage.GetTableName("JOB")}
                   SET EXPIRE_AT = :expireAt
                   WHERE ID = :id",
                new { id = long.Parse(jobId), expireAt = DateTime.UtcNow.Add(expireIn) },
                transaction: transaction,
                commandTimeout: _commandTimeout);
        });
    }

    /// <inheritdoc />
    public override void PersistJob(string jobId)
    {
        QueueCommand("PersistJob", (connection, transaction) =>
        {
            connection.Execute(
                $@"UPDATE {_storage.GetTableName("JOB")}
                   SET EXPIRE_AT = NULL
                   WHERE ID = :id",
                new { id = long.Parse(jobId) },
                transaction: transaction,
                commandTimeout: _commandTimeout);
        });
    }

    /// <inheritdoc />
    public override void SetJobState(string jobId, IState state)
    {
        QueueCommand("SetJobState", (connection, transaction) =>
        {
            // Use RETURNING to get the state ID in a single round-trip
            var stateTableName = _storage.GetTableName("JOB_STATE");
            var stateSeqName = _storage.GetTableName("JOB_STATE_SEQ");

            connection.Execute(
                $@"INSERT INTO {stateTableName} (ID, JOB_ID, NAME, REASON, CREATED_AT, DATA)
                   VALUES ({stateSeqName}.NEXTVAL, :jobId, :name, :reason, :createdAt, :data)",
                new
                {
                    jobId = long.Parse(jobId),
                    name = state.Name,
                    reason = state.Reason,
                    createdAt = DateTime.UtcNow,
                    data = SerializeStateData(state)
                },
                transaction: transaction,
                commandTimeout: _commandTimeout);

            var stateId = connection.ExecuteScalar<long>(
                $"SELECT {stateSeqName}.CURRVAL FROM DUAL",
                transaction: transaction,
                commandTimeout: _commandTimeout);

            connection.Execute(
                $@"UPDATE {_storage.GetTableName("JOB")}
                   SET STATE_ID = :stateId, STATE_NAME = :stateName
                   WHERE ID = :jobId",
                new { stateId, stateName = state.Name, jobId = long.Parse(jobId) },
                transaction: transaction,
                commandTimeout: _commandTimeout);
        });
    }

    /// <inheritdoc />
    public override void AddJobState(string jobId, IState state)
    {
        QueueCommand("AddJobState", (connection, transaction) =>
        {
            connection.Execute(
                $@"INSERT INTO {_storage.GetTableName("JOB_STATE")} (ID, JOB_ID, NAME, REASON, CREATED_AT, DATA)
                   VALUES ({_storage.GetTableName("JOB_STATE_SEQ")}.NEXTVAL, :jobId, :name, :reason, :createdAt, :data)",
                new
                {
                    jobId = long.Parse(jobId),
                    name = state.Name,
                    reason = state.Reason,
                    createdAt = DateTime.UtcNow,
                    data = SerializeStateData(state)
                },
                transaction: transaction,
                commandTimeout: _commandTimeout);
        });
    }

    /// <inheritdoc />
    public override void AddToQueue(string queue, string jobId)
    {
        QueueCommand("AddToQueue", (connection, transaction) =>
        {
            connection.Execute(
                $@"INSERT INTO {_storage.GetTableName("JOB_QUEUE")} (ID, JOB_ID, QUEUE, FETCHED_AT)
                   VALUES ({_storage.GetTableName("JOB_QUEUE_SEQ")}.NEXTVAL, :jobId, :queue, NULL)",
                new { jobId = long.Parse(jobId), queue },
                transaction: transaction,
                commandTimeout: _commandTimeout);
        });
    }

    /// <inheritdoc />
    public override void IncrementCounter(string key)
    {
        QueueCommand("IncrementCounter", (connection, transaction) =>
        {
            connection.Execute(
                $@"INSERT INTO {_storage.GetTableName("COUNTER")} (ID, KEY_NAME, VALUE, EXPIRE_AT)
                   VALUES ({_storage.GetTableName("COUNTER_SEQ")}.NEXTVAL, :key, 1, NULL)",
                new { key },
                transaction: transaction,
                commandTimeout: _commandTimeout);
        });
    }

    /// <inheritdoc />
    public override void IncrementCounter(string key, TimeSpan expireIn)
    {
        QueueCommand("IncrementCounter", (connection, transaction) =>
        {
            connection.Execute(
                $@"INSERT INTO {_storage.GetTableName("COUNTER")} (ID, KEY_NAME, VALUE, EXPIRE_AT)
                   VALUES ({_storage.GetTableName("COUNTER_SEQ")}.NEXTVAL, :key, 1, :expireAt)",
                new { key, expireAt = DateTime.UtcNow.Add(expireIn) },
                transaction: transaction,
                commandTimeout: _commandTimeout);
        });
    }

    /// <inheritdoc />
    public override void DecrementCounter(string key)
    {
        QueueCommand("DecrementCounter", (connection, transaction) =>
        {
            connection.Execute(
                $@"INSERT INTO {_storage.GetTableName("COUNTER")} (ID, KEY_NAME, VALUE, EXPIRE_AT)
                   VALUES ({_storage.GetTableName("COUNTER_SEQ")}.NEXTVAL, :key, -1, NULL)",
                new { key },
                transaction: transaction,
                commandTimeout: _commandTimeout);
        });
    }

    /// <inheritdoc />
    public override void DecrementCounter(string key, TimeSpan expireIn)
    {
        QueueCommand("DecrementCounter", (connection, transaction) =>
        {
            connection.Execute(
                $@"INSERT INTO {_storage.GetTableName("COUNTER")} (ID, KEY_NAME, VALUE, EXPIRE_AT)
                   VALUES ({_storage.GetTableName("COUNTER_SEQ")}.NEXTVAL, :key, -1, :expireAt)",
                new { key, expireAt = DateTime.UtcNow.Add(expireIn) },
                transaction: transaction,
                commandTimeout: _commandTimeout);
        });
    }

    /// <inheritdoc />
    public override void AddToSet(string key, string value)
    {
        AddToSet(key, value, 0.0);
    }

    /// <inheritdoc />
    public override void AddToSet(string key, string value, double score)
    {
        QueueCommand("AddToSet", (connection, transaction) =>
        {
            // Using MERGE for upsert - optimized for Oracle 19c+
            connection.Execute(
                $@"MERGE INTO {_storage.GetTableName("SET")} s
                   USING (SELECT :key AS KEY_NAME, :value AS VALUE FROM DUAL) src
                   ON (s.KEY_NAME = src.KEY_NAME AND s.VALUE = src.VALUE)
                   WHEN MATCHED THEN UPDATE SET s.SCORE = :score
                   WHEN NOT MATCHED THEN INSERT (ID, KEY_NAME, VALUE, SCORE, EXPIRE_AT)
                     VALUES ({_storage.GetTableName("SET_SEQ")}.NEXTVAL, :key, :value, :score, NULL)",
                new { key, value, score },
                transaction: transaction,
                commandTimeout: _commandTimeout);
        });
    }

    /// <inheritdoc />
    public override void RemoveFromSet(string key, string value)
    {
        QueueCommand("RemoveFromSet", (connection, transaction) =>
        {
            connection.Execute(
                $@"DELETE FROM {_storage.GetTableName("SET")}
                   WHERE KEY_NAME = :key AND VALUE = :value",
                new { key, value },
                transaction: transaction,
                commandTimeout: _commandTimeout);
        });
    }

    /// <inheritdoc />
    public override void InsertToList(string key, string value)
    {
        QueueCommand("InsertToList", (connection, transaction) =>
        {
            connection.Execute(
                $@"INSERT INTO {_storage.GetTableName("LIST")} (ID, KEY_NAME, VALUE, EXPIRE_AT)
                   VALUES ({_storage.GetTableName("LIST_SEQ")}.NEXTVAL, :key, :value, NULL)",
                new { key, value },
                transaction: transaction,
                commandTimeout: _commandTimeout);
        });
    }

    /// <inheritdoc />
    public override void RemoveFromList(string key, string value)
    {
        QueueCommand("RemoveFromList", (connection, transaction) =>
        {
            connection.Execute(
                $@"DELETE FROM {_storage.GetTableName("LIST")}
                   WHERE KEY_NAME = :key AND VALUE = :value",
                new { key, value },
                transaction: transaction,
                commandTimeout: _commandTimeout);
        });
    }

    /// <inheritdoc />
    public override void TrimList(string key, int keepStartingFrom, int keepEndingAt)
    {
        QueueCommand("TrimList", (connection, transaction) =>
        {
            // Use Oracle 19c+ OFFSET/FETCH syntax for clarity
            connection.Execute(
                $@"DELETE FROM {_storage.GetTableName("LIST")}
                   WHERE ID IN (
                     SELECT ID FROM (
                       SELECT ID, ROW_NUMBER() OVER (ORDER BY ID) AS RN
                       FROM {_storage.GetTableName("LIST")}
                       WHERE KEY_NAME = :key
                     )
                     WHERE RN < :startPos OR RN > :endPos
                   )",
                new { key, startPos = keepStartingFrom + 1, endPos = keepEndingAt + 1 },
                transaction: transaction,
                commandTimeout: _commandTimeout);
        });
    }

    /// <inheritdoc />
    public override void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(keyValuePairs);

        foreach (var kvp in keyValuePairs)
        {
            var field = kvp.Key;
            var value = kvp.Value;

            QueueCommand($"SetRangeInHash:{field}", (connection, transaction) =>
            {
                connection.Execute(
                    $@"MERGE INTO {_storage.GetTableName("HASH")} h
                       USING (SELECT :key AS KEY_NAME, :field AS FIELD FROM DUAL) src
                       ON (h.KEY_NAME = src.KEY_NAME AND h.FIELD = src.FIELD)
                       WHEN MATCHED THEN UPDATE SET h.VALUE = :value
                       WHEN NOT MATCHED THEN INSERT (ID, KEY_NAME, FIELD, VALUE, EXPIRE_AT)
                         VALUES ({_storage.GetTableName("HASH_SEQ")}.NEXTVAL, :key, :field, :value, NULL)",
                    new { key, field, value },
                    transaction: transaction,
                    commandTimeout: _commandTimeout);
            });
        }
    }

    /// <inheritdoc />
    public override void RemoveHash(string key)
    {
        ArgumentNullException.ThrowIfNull(key);

        QueueCommand("RemoveHash", (connection, transaction) =>
        {
            connection.Execute(
                $@"DELETE FROM {_storage.GetTableName("HASH")}
                   WHERE KEY_NAME = :key",
                new { key },
                transaction: transaction,
                commandTimeout: _commandTimeout);
        });
    }

    /// <inheritdoc />
    public override void ExpireSet(string key, TimeSpan expireIn)
    {
        ArgumentNullException.ThrowIfNull(key);

        QueueCommand("ExpireSet", (connection, transaction) =>
        {
            connection.Execute(
                $@"UPDATE {_storage.GetTableName("SET")}
                   SET EXPIRE_AT = :expireAt
                   WHERE KEY_NAME = :key",
                new { key, expireAt = DateTime.UtcNow.Add(expireIn) },
                transaction: transaction,
                commandTimeout: _commandTimeout);
        });
    }

    /// <inheritdoc />
    public override void ExpireHash(string key, TimeSpan expireIn)
    {
        ArgumentNullException.ThrowIfNull(key);

        QueueCommand("ExpireHash", (connection, transaction) =>
        {
            connection.Execute(
                $@"UPDATE {_storage.GetTableName("HASH")}
                   SET EXPIRE_AT = :expireAt
                   WHERE KEY_NAME = :key",
                new { key, expireAt = DateTime.UtcNow.Add(expireIn) },
                transaction: transaction,
                commandTimeout: _commandTimeout);
        });
    }

    /// <inheritdoc />
    public override void ExpireList(string key, TimeSpan expireIn)
    {
        ArgumentNullException.ThrowIfNull(key);

        QueueCommand("ExpireList", (connection, transaction) =>
        {
            connection.Execute(
                $@"UPDATE {_storage.GetTableName("LIST")}
                   SET EXPIRE_AT = :expireAt
                   WHERE KEY_NAME = :key",
                new { key, expireAt = DateTime.UtcNow.Add(expireIn) },
                transaction: transaction,
                commandTimeout: _commandTimeout);
        });
    }

    /// <inheritdoc />
    public override void PersistSet(string key)
    {
        ArgumentNullException.ThrowIfNull(key);

        QueueCommand("PersistSet", (connection, transaction) =>
        {
            connection.Execute(
                $@"UPDATE {_storage.GetTableName("SET")}
                   SET EXPIRE_AT = NULL
                   WHERE KEY_NAME = :key",
                new { key },
                transaction: transaction,
                commandTimeout: _commandTimeout);
        });
    }

    /// <inheritdoc />
    public override void PersistHash(string key)
    {
        ArgumentNullException.ThrowIfNull(key);

        QueueCommand("PersistHash", (connection, transaction) =>
        {
            connection.Execute(
                $@"UPDATE {_storage.GetTableName("HASH")}
                   SET EXPIRE_AT = NULL
                   WHERE KEY_NAME = :key",
                new { key },
                transaction: transaction,
                commandTimeout: _commandTimeout);
        });
    }

    /// <inheritdoc />
    public override void PersistList(string key)
    {
        ArgumentNullException.ThrowIfNull(key);

        QueueCommand("PersistList", (connection, transaction) =>
        {
            connection.Execute(
                $@"UPDATE {_storage.GetTableName("LIST")}
                   SET EXPIRE_AT = NULL
                   WHERE KEY_NAME = :key",
                new { key },
                transaction: transaction,
                commandTimeout: _commandTimeout);
        });
    }

    /// <summary>
    /// Serializes state data to JSON.
    /// </summary>
    private static string? SerializeStateData(IState state)
    {
        var data = state.SerializeData();
        return data.Count > 0 ? SerializationHelper.Serialize(data, SerializationOption.User) : null;
    }

    /// <summary>
    /// Queues a command for execution during commit.
    /// </summary>
    private void QueueCommand(string description, Action<OracleConnection, OracleTransaction> execute)
    {
        _commandQueue.Enqueue(new CommandAction
        {
            Description = description,
            Execute = execute
        });
    }
}
