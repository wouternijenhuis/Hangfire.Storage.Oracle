using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Dapper;
using Hangfire.Common;
using Hangfire.Server;
using Hangfire.Storage;
using Oracle.ManagedDataAccess.Client;

namespace Hangfire.Oracle.Core;

/// <summary>
/// Oracle storage connection implementation
/// </summary>
public class OracleStorageConnection : JobStorageConnection
{
    private readonly OracleStorage _storage;

    public OracleStorageConnection(OracleStorage storage)
    {
        _storage = storage ?? throw new ArgumentNullException(nameof(storage));
    }

    public override IWriteOnlyTransaction CreateWriteTransaction()
    {
        return new OracleWriteOnlyTransaction(_storage);
    }

    public override IDisposable AcquireDistributedLock(string resource, TimeSpan timeout)
    {
        return new OracleDistributedLock(_storage, resource, timeout);
    }

    public override IFetchedJob FetchNextJob(string[] queues, CancellationToken cancellationToken)
    {
        if (queues == null || queues.Length == 0)
            throw new ArgumentNullException(nameof(queues));

        var providers = queues.Select(queue => new OracleJobQueue(_storage, queue)).ToArray();

        var fetchedJob = default(IFetchedJob);
        while (fetchedJob == null)
        {
            cancellationToken.ThrowIfCancellationRequested();

            foreach (var provider in providers)
            {
                fetchedJob = provider.Dequeue(cancellationToken);
                if (fetchedJob != null)
                    break;
            }

            if (fetchedJob == null)
            {
                cancellationToken.WaitHandle.WaitOne(_storage.Options.QueuePollInterval);
            }
        }

        return fetchedJob;
    }

    public override string CreateExpiredJob(Job job, IDictionary<string, string> parameters, DateTime createdAt, TimeSpan expireIn)
    {
        if (job == null) throw new ArgumentNullException(nameof(job));

        var invocationData = InvocationData.SerializeJob(job);

        using var connection = _storage.CreateAndOpenConnection();
        using var transaction = connection.BeginTransaction();

        // Insert the job and get the ID using a SELECT after insert
        connection.Execute(
            $@"INSERT INTO {_storage.GetTableName("JOB")} (ID, INVOCATION_DATA, ARGUMENTS, CREATED_AT, EXPIRE_AT)
               VALUES ({_storage.GetTableName("JOB_SEQ")}.NEXTVAL, :invocationData, :arguments, :createdAt, :expireAt)",
            new
            {
                invocationData = JobHelper.ToJson(invocationData),
                arguments = invocationData.Arguments,
                createdAt = createdAt,
                expireAt = createdAt.Add(expireIn)
            },
            transaction: transaction);

        var jobId = connection.ExecuteScalar<long>(
            $@"SELECT {_storage.GetTableName("JOB_SEQ")}.CURRVAL FROM DUAL",
            transaction: transaction);

        if (parameters != null)
        {
            foreach (var parameter in parameters)
            {
                connection.Execute(
                    $@"INSERT INTO {_storage.GetTableName("JOB_PARAMETER")} (ID, JOB_ID, NAME, VALUE)
                       VALUES ({_storage.GetTableName("JOB_PARAMETER_SEQ")}.NEXTVAL, :jobId, :name, :value)",
                    new { jobId, name = parameter.Key, value = parameter.Value },
                    transaction: transaction);
            }
        }

        transaction.Commit();
        return jobId.ToString();
    }

    public override JobData GetJobData(string jobId)
    {
        if (jobId == null) throw new ArgumentNullException(nameof(jobId));

        using var connection = _storage.CreateAndOpenConnection();

        var jobData = connection.Query(
            $@"SELECT INVOCATION_DATA, ARGUMENTS, STATE_NAME, CREATED_AT, EXPIRE_AT
               FROM {_storage.GetTableName("JOB")}
               WHERE ID = :id",
            new { id = long.Parse(jobId) })
            .SingleOrDefault();

        if (jobData == null)
            return null;

        var invocationData = JobHelper.FromJson<InvocationData>(jobData.INVOCATION_DATA);
        invocationData.Arguments = jobData.ARGUMENTS;

        Job job = null;
        JobLoadException loadException = null;

        try
        {
            job = invocationData.DeserializeJob();
        }
        catch (JobLoadException ex)
        {
            loadException = ex;
        }

        return new JobData
        {
            Job = job,
            State = jobData.STATE_NAME,
            CreatedAt = jobData.CREATED_AT,
            LoadException = loadException
        };
    }

    public override StateData GetStateData(string jobId)
    {
        if (jobId == null) throw new ArgumentNullException(nameof(jobId));

        using var connection = _storage.CreateAndOpenConnection();

        var stateData = connection.Query(
            $@"SELECT s.NAME, s.REASON, s.DATA
               FROM {_storage.GetTableName("JOB_STATE")} s
               INNER JOIN {_storage.GetTableName("JOB")} j ON j.STATE_ID = s.ID
               WHERE j.ID = :id",
            new { id = long.Parse(jobId) })
            .SingleOrDefault();

        if (stateData == null)
            return null;

        return new StateData
        {
            Name = stateData.NAME,
            Reason = stateData.REASON,
            Data = JobHelper.FromJson<Dictionary<string, string>>(stateData.DATA)
        };
    }

    public override void SetJobParameter(string id, string name, string value)
    {
        if (id == null) throw new ArgumentNullException(nameof(id));
        if (name == null) throw new ArgumentNullException(nameof(name));

        using var connection = _storage.CreateAndOpenConnection();

        connection.Execute(
            $@"MERGE INTO {_storage.GetTableName("JOB_PARAMETER")} jp
               USING (SELECT :jobId AS JOB_ID, :name AS NAME FROM DUAL) src
               ON (jp.JOB_ID = src.JOB_ID AND jp.NAME = src.NAME)
               WHEN MATCHED THEN UPDATE SET jp.VALUE = :value
               WHEN NOT MATCHED THEN INSERT (ID, JOB_ID, NAME, VALUE)
                 VALUES ({_storage.GetTableName("JOB_PARAMETER_SEQ")}.NEXTVAL, :jobId, :name, :value)",
            new { jobId = long.Parse(id), name, value });
    }

    public override string GetJobParameter(string id, string name)
    {
        if (id == null) throw new ArgumentNullException(nameof(id));
        if (name == null) throw new ArgumentNullException(nameof(name));

        using var connection = _storage.CreateAndOpenConnection();

        return connection.ExecuteScalar<string>(
            $@"SELECT VALUE FROM {_storage.GetTableName("JOB_PARAMETER")}
               WHERE JOB_ID = :jobId AND NAME = :name",
            new { jobId = long.Parse(id), name });
    }

    public override HashSet<string> GetAllItemsFromSet(string key)
    {
        if (key == null) throw new ArgumentNullException(nameof(key));

        using var connection = _storage.CreateAndOpenConnection();

        var result = connection.Query<string>(
            $@"SELECT VALUE FROM {_storage.GetTableName("SET")}
               WHERE KEY_NAME = :key",
            new { key });

        return new HashSet<string>(result);
    }

    public override string GetFirstByLowestScoreFromSet(string key, double fromScore, double toScore)
    {
        if (key == null) throw new ArgumentNullException(nameof(key));

        using var connection = _storage.CreateAndOpenConnection();

        return connection.Query<string>(
            $@"SELECT VALUE FROM (
                 SELECT VALUE FROM {_storage.GetTableName("SET")}
                 WHERE KEY_NAME = :key AND SCORE BETWEEN :from AND :to
                 ORDER BY SCORE
               ) WHERE ROWNUM = 1",
            new { key, from = fromScore, to = toScore })
            .SingleOrDefault();
    }

    public override long GetCounter(string key)
    {
        if (key == null) throw new ArgumentNullException(nameof(key));

        using var connection = _storage.CreateAndOpenConnection();

        var result = connection.ExecuteScalar<long?>(
            $@"SELECT SUM(VALUE) FROM {_storage.GetTableName("COUNTER")}
               WHERE KEY_NAME = :key",
            new { key });

        return result ?? 0;
    }

    public override long GetSetCount(string key)
    {
        if (key == null) throw new ArgumentNullException(nameof(key));

        using var connection = _storage.CreateAndOpenConnection();

        return connection.ExecuteScalar<long>(
            $@"SELECT COUNT(*) FROM {_storage.GetTableName("SET")}
               WHERE KEY_NAME = :key",
            new { key });
    }

    public override List<string> GetRangeFromSet(string key, int startingFrom, int endingAt)
    {
        if (key == null) throw new ArgumentNullException(nameof(key));

        using var connection = _storage.CreateAndOpenConnection();

        var result = connection.Query<string>(
            $@"SELECT VALUE FROM (
                 SELECT VALUE, ROW_NUMBER() OVER (ORDER BY ID) AS ROWNUM
                 FROM {_storage.GetTableName("SET")}
                 WHERE KEY_NAME = :key
               )
               WHERE ROWNUM > :start AND ROWNUM <= :end",
            new { key, start = startingFrom, end = endingAt + 1 });

        return result.ToList();
    }

    public override TimeSpan GetSetTtl(string key)
    {
        if (key == null) throw new ArgumentNullException(nameof(key));

        using var connection = _storage.CreateAndOpenConnection();

        var result = connection.ExecuteScalar<DateTime?>(
            $@"SELECT MIN(EXPIRE_AT) FROM {_storage.GetTableName("SET")}
               WHERE KEY_NAME = :key",
            new { key });

        if (!result.HasValue)
            return TimeSpan.FromSeconds(-1);

        return result.Value - DateTime.UtcNow;
    }

    public override long GetHashCount(string key)
    {
        if (key == null) throw new ArgumentNullException(nameof(key));

        using var connection = _storage.CreateAndOpenConnection();

        return connection.ExecuteScalar<long>(
            $@"SELECT COUNT(*) FROM {_storage.GetTableName("HASH")}
               WHERE KEY_NAME = :key",
            new { key });
    }

    public override TimeSpan GetHashTtl(string key)
    {
        if (key == null) throw new ArgumentNullException(nameof(key));

        using var connection = _storage.CreateAndOpenConnection();

        var result = connection.ExecuteScalar<DateTime?>(
            $@"SELECT MIN(EXPIRE_AT) FROM {_storage.GetTableName("HASH")}
               WHERE KEY_NAME = :key",
            new { key });

        if (!result.HasValue)
            return TimeSpan.FromSeconds(-1);

        return result.Value - DateTime.UtcNow;
    }

    public override string GetValueFromHash(string key, string name)
    {
        if (key == null) throw new ArgumentNullException(nameof(key));
        if (name == null) throw new ArgumentNullException(nameof(name));

        using var connection = _storage.CreateAndOpenConnection();

        return connection.ExecuteScalar<string>(
            $@"SELECT VALUE FROM {_storage.GetTableName("HASH")}
               WHERE KEY_NAME = :key AND FIELD = :field",
            new { key, field = name });
    }

    public override long GetListCount(string key)
    {
        if (key == null) throw new ArgumentNullException(nameof(key));

        using var connection = _storage.CreateAndOpenConnection();

        return connection.ExecuteScalar<long>(
            $@"SELECT COUNT(*) FROM {_storage.GetTableName("LIST")}
               WHERE KEY_NAME = :key",
            new { key });
    }

    public override TimeSpan GetListTtl(string key)
    {
        if (key == null) throw new ArgumentNullException(nameof(key));

        using var connection = _storage.CreateAndOpenConnection();

        var result = connection.ExecuteScalar<DateTime?>(
            $@"SELECT MIN(EXPIRE_AT) FROM {_storage.GetTableName("LIST")}
               WHERE KEY_NAME = :key",
            new { key });

        if (!result.HasValue)
            return TimeSpan.FromSeconds(-1);

        return result.Value - DateTime.UtcNow;
    }

    public override List<string> GetRangeFromList(string key, int startingFrom, int endingAt)
    {
        if (key == null) throw new ArgumentNullException(nameof(key));

        using var connection = _storage.CreateAndOpenConnection();

        var result = connection.Query<string>(
            $@"SELECT VALUE FROM (
                 SELECT VALUE, ROW_NUMBER() OVER (ORDER BY ID) AS ROWNUM
                 FROM {_storage.GetTableName("LIST")}
                 WHERE KEY_NAME = :key
               )
               WHERE ROWNUM > :start AND ROWNUM <= :end",
            new { key, start = startingFrom, end = endingAt + 1 });

        return result.ToList();
    }

    public override List<string> GetAllItemsFromList(string key)
    {
        if (key == null) throw new ArgumentNullException(nameof(key));

        using var connection = _storage.CreateAndOpenConnection();

        var result = connection.Query<string>(
            $@"SELECT VALUE FROM {_storage.GetTableName("LIST")}
               WHERE KEY_NAME = :key
               ORDER BY ID",
            new { key });

        return result.ToList();
    }

    public override Dictionary<string, string> GetAllEntriesFromHash(string key)
    {
        if (key == null) throw new ArgumentNullException(nameof(key));

        using var connection = _storage.CreateAndOpenConnection();

        var result = connection.Query(
            $@"SELECT FIELD, VALUE FROM {_storage.GetTableName("HASH")}
               WHERE KEY_NAME = :key",
            new { key })
            .ToDictionary(
                x => (string)x.FIELD,
                x => (string)x.VALUE);

        return result;
    }

    public override void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
    {
        if (key == null) throw new ArgumentNullException(nameof(key));
        if (keyValuePairs == null) throw new ArgumentNullException(nameof(keyValuePairs));

        using var connection = _storage.CreateAndOpenConnection();

        foreach (var pair in keyValuePairs)
        {
            connection.Execute(
                $@"MERGE INTO {_storage.GetTableName("HASH")} h
                   USING (SELECT :key AS KEY_NAME, :field AS FIELD FROM DUAL) src
                   ON (h.KEY_NAME = src.KEY_NAME AND h.FIELD = src.FIELD)
                   WHEN MATCHED THEN UPDATE SET h.VALUE = :value
                   WHEN NOT MATCHED THEN INSERT (ID, KEY_NAME, FIELD, VALUE, EXPIRE_AT)
                     VALUES ({_storage.GetTableName("HASH_SEQ")}.NEXTVAL, :key, :field, :value, NULL)",
                new { key, field = pair.Key, value = pair.Value });
        }
    }

    public override void AnnounceServer(string serverId, ServerContext context)
    {
        if (serverId == null) throw new ArgumentNullException(nameof(serverId));
        if (context == null) throw new ArgumentNullException(nameof(context));

        using var connection = _storage.CreateAndOpenConnection();

        var data = JobHelper.ToJson(new
        {
            context.WorkerCount,
            context.Queues,
            StartedAt = DateTime.UtcNow
        });

        connection.Execute(
            $@"MERGE INTO {_storage.GetTableName("SERVER")} s
               USING (SELECT :id AS ID FROM DUAL) src
               ON (s.ID = src.ID)
               WHEN MATCHED THEN UPDATE SET s.DATA = :data, s.LAST_HEARTBEAT = :now
               WHEN NOT MATCHED THEN INSERT (ID, DATA, LAST_HEARTBEAT)
                 VALUES (:id, :data, :now)",
            new { id = serverId, data, now = DateTime.UtcNow });
    }

    public override void RemoveServer(string serverId)
    {
        if (serverId == null) throw new ArgumentNullException(nameof(serverId));

        using var connection = _storage.CreateAndOpenConnection();

        connection.Execute(
            $@"DELETE FROM {_storage.GetTableName("SERVER")}
               WHERE ID = :id",
            new { id = serverId });
    }

    public override void Heartbeat(string serverId)
    {
        if (serverId == null) throw new ArgumentNullException(nameof(serverId));

        using var connection = _storage.CreateAndOpenConnection();

        connection.Execute(
            $@"UPDATE {_storage.GetTableName("SERVER")}
               SET LAST_HEARTBEAT = :now
               WHERE ID = :id",
            new { id = serverId, now = DateTime.UtcNow });
    }

    public override int RemoveTimedOutServers(TimeSpan timeOut)
    {
        if (timeOut.Duration() != timeOut)
        {
            throw new ArgumentException("The `timeOut` value must be positive.", nameof(timeOut));
        }

        using var connection = _storage.CreateAndOpenConnection();

        return connection.Execute(
            $@"DELETE FROM {_storage.GetTableName("SERVER")}
               WHERE LAST_HEARTBEAT < :timeOutAt",
            new { timeOutAt = DateTime.UtcNow.Add(timeOut.Negate()) });
    }
}
