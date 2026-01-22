using System;
using System.Collections.Generic;
using System.Linq;
using Dapper;
using Hangfire.Common;
using Hangfire.States;
using Hangfire.Storage;
using Oracle.ManagedDataAccess.Client;

namespace Hangfire.Oracle.Core;

/// <summary>
/// Oracle write-only transaction implementation
/// </summary>
public class OracleWriteOnlyTransaction : JobStorageTransaction
{
    private readonly OracleStorage _storage;
    private readonly Queue<Action<OracleConnection, OracleTransaction>> _commandQueue = new();

    public OracleWriteOnlyTransaction(OracleStorage storage)
    {
        _storage = storage ?? throw new ArgumentNullException(nameof(storage));
    }

    public override void Commit()
    {
        using var connection = _storage.CreateAndOpenConnection();
        using var transaction = connection.BeginTransaction();

        foreach (var command in _commandQueue)
        {
            command(connection, transaction);
        }

        transaction.Commit();
    }

    public override void ExpireJob(string jobId, TimeSpan expireIn)
    {
        QueueCommand((connection, transaction) =>
        {
            connection.Execute(
                $@"UPDATE {_storage.GetTableName("JOB")}
                   SET EXPIRE_AT = :expireAt
                   WHERE ID = :id",
                new { id = long.Parse(jobId), expireAt = DateTime.UtcNow.Add(expireIn) },
                transaction: transaction);
        });
    }

    public override void PersistJob(string jobId)
    {
        QueueCommand((connection, transaction) =>
        {
            connection.Execute(
                $@"UPDATE {_storage.GetTableName("JOB")}
                   SET EXPIRE_AT = NULL
                   WHERE ID = :id",
                new { id = long.Parse(jobId) },
                transaction: transaction);
        });
    }

    public override void SetJobState(string jobId, IState state)
    {
        QueueCommand((connection, transaction) =>
        {
            var stateId = connection.ExecuteScalar<long>(
                $@"INSERT INTO {_storage.GetTableName("JOB_STATE")} (ID, JOB_ID, NAME, REASON, CREATED_AT, DATA)
                   VALUES ({_storage.GetTableName("JOB_STATE_SEQ")}.NEXTVAL, :jobId, :name, :reason, :createdAt, :data)
                   RETURNING ID INTO :id",
                new
                {
                    jobId = long.Parse(jobId),
                    name = state.Name,
                    reason = state.Reason,
                    createdAt = DateTime.UtcNow,
                    data = JobHelper.ToJson(state.SerializeData())
                },
                transaction: transaction);

            connection.Execute(
                $@"UPDATE {_storage.GetTableName("JOB")}
                   SET STATE_ID = :stateId, STATE_NAME = :stateName
                   WHERE ID = :jobId",
                new { stateId, stateName = state.Name, jobId = long.Parse(jobId) },
                transaction: transaction);
        });
    }

    public override void AddJobState(string jobId, IState state)
    {
        QueueCommand((connection, transaction) =>
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
                    data = JobHelper.ToJson(state.SerializeData())
                },
                transaction: transaction);
        });
    }

    public override void AddToQueue(string queue, string jobId)
    {
        QueueCommand((connection, transaction) =>
        {
            connection.Execute(
                $@"INSERT INTO {_storage.GetTableName("JOB_QUEUE")} (ID, JOB_ID, QUEUE, FETCHED_AT)
                   VALUES ({_storage.GetTableName("JOB_QUEUE_SEQ")}.NEXTVAL, :jobId, :queue, NULL)",
                new { jobId = long.Parse(jobId), queue },
                transaction: transaction);
        });
    }

    public override void IncrementCounter(string key)
    {
        QueueCommand((connection, transaction) =>
        {
            connection.Execute(
                $@"INSERT INTO {_storage.GetTableName("COUNTER")} (ID, KEY_NAME, VALUE, EXPIRE_AT)
                   VALUES ({_storage.GetTableName("COUNTER_SEQ")}.NEXTVAL, :key, 1, NULL)",
                new { key },
                transaction: transaction);
        });
    }

    public override void IncrementCounter(string key, TimeSpan expireIn)
    {
        QueueCommand((connection, transaction) =>
        {
            connection.Execute(
                $@"INSERT INTO {_storage.GetTableName("COUNTER")} (ID, KEY_NAME, VALUE, EXPIRE_AT)
                   VALUES ({_storage.GetTableName("COUNTER_SEQ")}.NEXTVAL, :key, 1, :expireAt)",
                new { key, expireAt = DateTime.UtcNow.Add(expireIn) },
                transaction: transaction);
        });
    }

    public override void DecrementCounter(string key)
    {
        QueueCommand((connection, transaction) =>
        {
            connection.Execute(
                $@"INSERT INTO {_storage.GetTableName("COUNTER")} (ID, KEY_NAME, VALUE, EXPIRE_AT)
                   VALUES ({_storage.GetTableName("COUNTER_SEQ")}.NEXTVAL, :key, -1, NULL)",
                new { key },
                transaction: transaction);
        });
    }

    public override void DecrementCounter(string key, TimeSpan expireIn)
    {
        QueueCommand((connection, transaction) =>
        {
            connection.Execute(
                $@"INSERT INTO {_storage.GetTableName("COUNTER")} (ID, KEY_NAME, VALUE, EXPIRE_AT)
                   VALUES ({_storage.GetTableName("COUNTER_SEQ")}.NEXTVAL, :key, -1, :expireAt)",
                new { key, expireAt = DateTime.UtcNow.Add(expireIn) },
                transaction: transaction);
        });
    }

    public override void AddToSet(string key, string value)
    {
        AddToSet(key, value, 0.0);
    }

    public override void AddToSet(string key, string value, double score)
    {
        QueueCommand((connection, transaction) =>
        {
            connection.Execute(
                $@"MERGE INTO {_storage.GetTableName("SET")} s
                   USING (SELECT :key AS KEY_NAME, :value AS VALUE FROM DUAL) src
                   ON (s.KEY_NAME = src.KEY_NAME AND s.VALUE = src.VALUE)
                   WHEN MATCHED THEN UPDATE SET s.SCORE = :score
                   WHEN NOT MATCHED THEN INSERT (ID, KEY_NAME, VALUE, SCORE, EXPIRE_AT)
                     VALUES ({_storage.GetTableName("SET_SEQ")}.NEXTVAL, :key, :value, :score, NULL)",
                new { key, value, score },
                transaction: transaction);
        });
    }

    public override void RemoveFromSet(string key, string value)
    {
        QueueCommand((connection, transaction) =>
        {
            connection.Execute(
                $@"DELETE FROM {_storage.GetTableName("SET")}
                   WHERE KEY_NAME = :key AND VALUE = :value",
                new { key, value },
                transaction: transaction);
        });
    }

    public override void InsertToList(string key, string value)
    {
        QueueCommand((connection, transaction) =>
        {
            connection.Execute(
                $@"INSERT INTO {_storage.GetTableName("LIST")} (ID, KEY_NAME, VALUE, EXPIRE_AT)
                   VALUES ({_storage.GetTableName("LIST_SEQ")}.NEXTVAL, :key, :value, NULL)",
                new { key, value },
                transaction: transaction);
        });
    }

    public override void RemoveFromList(string key, string value)
    {
        QueueCommand((connection, transaction) =>
        {
            connection.Execute(
                $@"DELETE FROM {_storage.GetTableName("LIST")}
                   WHERE KEY_NAME = :key AND VALUE = :value",
                new { key, value },
                transaction: transaction);
        });
    }

    public override void TrimList(string key, int keepStartingFrom, int keepEndingAt)
    {
        QueueCommand((connection, transaction) =>
        {
            connection.Execute(
                $@"DELETE FROM {_storage.GetTableName("LIST")}
                   WHERE ID IN (
                     SELECT ID FROM (
                       SELECT ID, ROW_NUMBER() OVER (ORDER BY ID) AS ROWNUM
                       FROM {_storage.GetTableName("LIST")}
                       WHERE KEY_NAME = :key
                     )
                     WHERE ROWNUM < :start OR ROWNUM > :end
                   )",
                new { key, start = keepStartingFrom + 1, end = keepEndingAt + 1 },
                transaction: transaction);
        });
    }

    public override void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
    {
        if (key == null) throw new ArgumentNullException(nameof(key));
        if (keyValuePairs == null) throw new ArgumentNullException(nameof(keyValuePairs));

        foreach (var keyValuePair in keyValuePairs)
        {
            QueueCommand((connection, transaction) =>
            {
                connection.Execute(
                    $@"MERGE INTO {_storage.GetTableName("HASH")} h
                       USING (SELECT :key AS KEY_NAME, :field AS FIELD FROM DUAL) src
                       ON (h.KEY_NAME = src.KEY_NAME AND h.FIELD = src.FIELD)
                       WHEN MATCHED THEN UPDATE SET h.VALUE = :value
                       WHEN NOT MATCHED THEN INSERT (ID, KEY_NAME, FIELD, VALUE, EXPIRE_AT)
                         VALUES ({_storage.GetTableName("HASH_SEQ")}.NEXTVAL, :key, :field, :value, NULL)",
                    new { key, field = keyValuePair.Key, value = keyValuePair.Value },
                    transaction: transaction);
            });
        }
    }

    public override void RemoveHash(string key)
    {
        if (key == null) throw new ArgumentNullException(nameof(key));

        QueueCommand((connection, transaction) =>
        {
            connection.Execute(
                $@"DELETE FROM {_storage.GetTableName("HASH")}
                   WHERE KEY_NAME = :key",
                new { key },
                transaction: transaction);
        });
    }

    public override void ExpireSet(string key, TimeSpan expireIn)
    {
        if (key == null) throw new ArgumentNullException(nameof(key));

        QueueCommand((connection, transaction) =>
        {
            connection.Execute(
                $@"UPDATE {_storage.GetTableName("SET")}
                   SET EXPIRE_AT = :expireAt
                   WHERE KEY_NAME = :key",
                new { key, expireAt = DateTime.UtcNow.Add(expireIn) },
                transaction: transaction);
        });
    }

    public override void ExpireHash(string key, TimeSpan expireIn)
    {
        if (key == null) throw new ArgumentNullException(nameof(key));

        QueueCommand((connection, transaction) =>
        {
            connection.Execute(
                $@"UPDATE {_storage.GetTableName("HASH")}
                   SET EXPIRE_AT = :expireAt
                   WHERE KEY_NAME = :key",
                new { key, expireAt = DateTime.UtcNow.Add(expireIn) },
                transaction: transaction);
        });
    }

    public override void ExpireList(string key, TimeSpan expireIn)
    {
        if (key == null) throw new ArgumentNullException(nameof(key));

        QueueCommand((connection, transaction) =>
        {
            connection.Execute(
                $@"UPDATE {_storage.GetTableName("LIST")}
                   SET EXPIRE_AT = :expireAt
                   WHERE KEY_NAME = :key",
                new { key, expireAt = DateTime.UtcNow.Add(expireIn) },
                transaction: transaction);
        });
    }

    public override void PersistSet(string key)
    {
        if (key == null) throw new ArgumentNullException(nameof(key));

        QueueCommand((connection, transaction) =>
        {
            connection.Execute(
                $@"UPDATE {_storage.GetTableName("SET")}
                   SET EXPIRE_AT = NULL
                   WHERE KEY_NAME = :key",
                new { key },
                transaction: transaction);
        });
    }

    public override void PersistHash(string key)
    {
        if (key == null) throw new ArgumentNullException(nameof(key));

        QueueCommand((connection, transaction) =>
        {
            connection.Execute(
                $@"UPDATE {_storage.GetTableName("HASH")}
                   SET EXPIRE_AT = NULL
                   WHERE KEY_NAME = :key",
                new { key },
                transaction: transaction);
        });
    }

    public override void PersistList(string key)
    {
        if (key == null) throw new ArgumentNullException(nameof(key));

        QueueCommand((connection, transaction) =>
        {
            connection.Execute(
                $@"UPDATE {_storage.GetTableName("LIST")}
                   SET EXPIRE_AT = NULL
                   WHERE KEY_NAME = :key",
                new { key },
                transaction: transaction);
        });
    }

    private void QueueCommand(Action<OracleConnection, OracleTransaction> command)
    {
        _commandQueue.Enqueue(command);
    }
}
