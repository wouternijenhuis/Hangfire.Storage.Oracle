using Dapper;
using Hangfire.Common;
using Hangfire.Oracle.Core.BackgroundProcesses;
using Hangfire.Oracle.Core.Schema;
using Hangfire.Server;
using Hangfire.States;
using Oracle.ManagedDataAccess.Client;

namespace Hangfire.Oracle.Core.Tests;

public class OracleIntegrationTests
{
    private static readonly string[] _defaultQueue = { "default" };

    [Fact]
    [Trait("Category", "OracleIntegration")]
    public async Task StorageContractWorksAgainstOracleDatabase()
    {
        var connectionString = Environment.GetEnvironmentVariable("ORACLE_TEST_CONNECTION_STRING");
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            return;
        }

        var prefix = $"T{Guid.NewGuid():N}"[..10].ToUpperInvariant() + "_";
        using var admin = new OracleConnection(connectionString);
        admin.Open();

        try
        {
            OracleSchemaManager.EnsureSchemaCreated(admin, prefix);
            OracleSchemaManager.EnsureSchemaCreated(admin, prefix);
            Assert.True(OracleSchemaManager.TablesExist(admin, prefix));
            Assert.Equal(2, OracleSchemaManager.GetSchemaVersion(admin, prefix));

            await ExerciseStorage(connectionString, prefix);
            ExerciseMigration(admin, prefix);
        }
        finally
        {
            OracleSchemaManager.DropSchema(admin, prefix);
        }
    }

    private static async Task ExerciseStorage(string connectionString, string prefix)
    {
        var options = new OracleStorageOptions
        {
            SchemaName = new OracleConnectionStringBuilder(connectionString).UserID.ToUpperInvariant(),
            TablePrefix = prefix,
            PrepareSchemaIfNecessary = false,
            QueuePollInterval = TimeSpan.FromMilliseconds(20),
            SlidingInvisibilityTimeout = TimeSpan.FromSeconds(5),
            InvisibilityTimeout = TimeSpan.FromMinutes(1),
            DistributedLockTimeout = TimeSpan.FromSeconds(10)
        };
        using var storage = new OracleStorage(connectionString, options);
        var connection = new OracleStorageConnection(storage);
        var job = Job.FromExpression(() => OracleIntegrationJob.Execute());
        var jobId = connection.CreateExpiredJob(
            job,
            new Dictionary<string, string> { ["source"] = "integration" },
            DateTime.UtcNow,
            TimeSpan.FromHours(1));

        Assert.NotNull(connection.GetJobData(jobId));
        Assert.Equal("integration", connection.GetJobParameter(jobId, "source"));

        using (var transaction = connection.CreateWriteTransaction())
        {
            transaction.SetJobState(jobId, new EnqueuedState("default"));
            transaction.AddToQueue("default", jobId);
            transaction.IncrementCounter("stats:test");
            transaction.AddToSet("test-set", "value", 1);
            transaction.InsertToList("test-list", "value");
            transaction.SetRangeInHash("test-hash", new Dictionary<string, string> { ["field"] = "value" });
            transaction.Commit();
        }

        Assert.Equal(1, connection.GetCounter("stats:test"));
        Assert.Equal(1, connection.GetSetCount("test-set"));
        Assert.Equal("value", connection.GetValueFromHash("test-hash", "field"));
        Assert.Equal("value", Assert.Single(connection.GetAllItemsFromList("test-list")));
        Assert.Contains(storage.GetMonitoringApi().Queues(), queue => queue.Name == "default");
        Assert.NotNull(storage.GetMonitoringApi().JobDetails(jobId));
        Assert.Single(storage.GetMonitoringApi().EnqueuedJobs("default", 0, 10));

        var monitor = storage.QueueProviders.DefaultProvider.GetMonitor();
        Assert.Contains("default", monitor.GetAllQueues());
        Assert.Equal(1, monitor.GetStatistics("default").EnqueuedCount);
        Assert.Contains(long.Parse(jobId), monitor.GetEnqueuedJobIds("default", 0, 10));

        using (var fetched = connection.FetchNextJob(_defaultQueue, new CancellationTokenSource(TimeSpan.FromSeconds(10)).Token))
        {
            Assert.Equal(jobId, fetched.JobId);
            Assert.Single(storage.GetMonitoringApi().FetchedJobs("default", 0, 10));
            fetched.Requeue();
        }

        using (var fetched = connection.FetchNextJob(_defaultQueue, new CancellationTokenSource(TimeSpan.FromSeconds(10)).Token))
        {
            fetched.RemoveFromQueue();
        }

        using (var canceled = new CancellationTokenSource())
        {
            canceled.Cancel();
            Assert.Throws<OperationCanceledException>(() => connection.FetchNextJob(_defaultQueue, canceled.Token));
        }

        await ExerciseConcurrentQueues(storage, connection, job);

        using (var firstLock = connection.AcquireDistributedLock("integration-lock", TimeSpan.FromSeconds(2)))
        {
            Assert.Throws<Hangfire.Oracle.Core.Exceptions.DistributedLockAcquisitionException>(
                () => connection.AcquireDistributedLock("integration-lock", TimeSpan.FromMilliseconds(100)));
        }

        using var secondLock = connection.AcquireDistributedLock("integration-lock", TimeSpan.FromSeconds(2));
        ExerciseStaleLockOwnership(storage, connection);
        var aggregator = new CounterAggregationProcess(storage, TimeSpan.FromMinutes(1), 100);
        Assert.Equal(1, aggregator.ProcessBatch());
        Assert.Equal(1, connection.GetCounter("stats:test"));

        await ExerciseAsyncOperations(storage);
        ExerciseCollectionsAndCleanup(storage, connection, job);
        ExerciseServersAndMonitoring(storage, connection, job);
        await ExerciseEndToEndJob(storage, connection);
    }

    private static async Task ExerciseConcurrentQueues(
        OracleStorage storage,
        OracleStorageConnection connection,
        Job job)
    {
        var jobIds = new[]
        {
            connection.CreateExpiredJob(job, new Dictionary<string, string>(), DateTime.UtcNow, TimeSpan.FromHours(1)),
            connection.CreateExpiredJob(job, new Dictionary<string, string>(), DateTime.UtcNow, TimeSpan.FromHours(1))
        };
        using (var transaction = connection.CreateWriteTransaction())
        {
            transaction.SetJobState(jobIds[0], new EnqueuedState("default"));
            transaction.AddToQueue("default", jobIds[0]);
            transaction.SetJobState(jobIds[1], new EnqueuedState("critical"));
            transaction.AddToQueue("critical", jobIds[1]);
            transaction.Commit();
        }

        var queues = new[] { "default", "critical" };
        var fetchTasks = Enumerable.Range(0, 2).Select(_ => Task.Run(() =>
        {
            using var workerConnection = new OracleStorageConnection(storage);
            using var fetched = workerConnection.FetchNextJob(
                queues,
                new CancellationTokenSource(TimeSpan.FromSeconds(10)).Token);
            var fetchedJobId = fetched.JobId;
            fetched.RemoveFromQueue();
            return fetchedJobId;
        }));
        var fetchedJobIds = await Task.WhenAll(fetchTasks);

        Assert.Equal(2, fetchedJobIds.Distinct(StringComparer.Ordinal).Count());
        Assert.All(jobIds, id => Assert.Contains(id, fetchedJobIds));
    }

    private static void ExerciseStaleLockOwnership(OracleStorage storage, OracleStorageConnection connection)
    {
        var staleOwner = connection.AcquireDistributedLock("stale-owner-lock", TimeSpan.FromSeconds(2));
        storage.ExecuteWithConnection(database => database.Execute(
            $"UPDATE {storage.GetTableName("DISTRIBUTED_LOCK")} SET EXPIRE_AT = :expireAt WHERE RESOURCE_NAME = :resourceName",
            new { expireAt = storage.GetUtcOrLocalNow().AddSeconds(-1), resourceName = "stale-owner-lock" }));

        using var replacementOwner = connection.AcquireDistributedLock("stale-owner-lock", TimeSpan.FromSeconds(2));
        staleOwner.Dispose();
        Assert.Throws<Hangfire.Oracle.Core.Exceptions.DistributedLockAcquisitionException>(
            () => connection.AcquireDistributedLock("stale-owner-lock", TimeSpan.FromMilliseconds(100)));
    }

    private static async Task ExerciseEndToEndJob(OracleStorage storage, OracleStorageConnection connection)
    {
        OracleIntegrationJob.Reset();
        using var server = new BackgroundJobServer(
            new BackgroundJobServerOptions
            {
                ServerName = "oracle-integration-worker",
                WorkerCount = 1,
                Queues = _defaultQueue,
                SchedulePollingInterval = TimeSpan.FromMilliseconds(100)
            },
            storage);
        var client = new BackgroundJobClient(storage);
        var jobId = client.Enqueue(() => OracleIntegrationJob.Execute());

        await OracleIntegrationJob.WaitAsync(TimeSpan.FromSeconds(20));
        var deadline = DateTime.UtcNow.AddSeconds(10);
        while (!string.Equals(connection.GetStateData(jobId)?.Name, "Succeeded", StringComparison.Ordinal)
               && DateTime.UtcNow < deadline)
        {
            await Task.Delay(50);
        }

        Assert.Equal("Succeeded", connection.GetStateData(jobId)?.Name);
    }

    private static async Task ExerciseAsyncOperations(OracleStorage storage)
    {
        Assert.Equal(1, await storage.ExecuteScalarWithRetryAsync<int>("SELECT 1 FROM DUAL"));
        Assert.Equal(1, Assert.Single(await storage.QueryWithRetryAsync<int>("SELECT 1 FROM DUAL")));
        Assert.Equal(1, await storage.QuerySingleOrDefaultWithRetryAsync<int>("SELECT 1 FROM DUAL"));
        Assert.Equal(1, await storage.ExecuteWithConnectionAsync(connection => connection.ExecuteScalarAsync<int>("SELECT 1 FROM DUAL")));
        Assert.Equal(1, await storage.ExecuteInTransactionAsync((connection, _) => connection.ExecuteScalarAsync<int>("SELECT 1 FROM DUAL")));
        Assert.Equal(1, await storage.ExecuteInTransactionWithRetryAsync((connection, _, _) => connection.ExecuteScalarAsync<int>("SELECT 1 FROM DUAL")));

        var streamed = new List<int>();
        await foreach (var value in storage.QueryStreamAsync<int>("SELECT 1 FROM DUAL"))
        {
            streamed.Add(value);
        }

        Assert.Equal(1, Assert.Single(streamed));
    }

    private static void ExerciseCollectionsAndCleanup(
        OracleStorage storage,
        OracleStorageConnection connection,
        Job job)
    {
        var expiringJobId = connection.CreateExpiredJob(job, new Dictionary<string, string>(), DateTime.UtcNow, TimeSpan.FromSeconds(-1));
        var transaction = new OracleWriteOnlyTransaction(storage);
        transaction.IncrementCounter("expiring-counter", TimeSpan.FromSeconds(-1));
        transaction.DecrementCounter("balanced-counter");
        transaction.IncrementCounter("balanced-counter");
        transaction.DecrementCounter("balanced-expiring", TimeSpan.FromMinutes(1));
        transaction.AddToSet("expiring-set", "a", 2);
        transaction.AddToSet("expiring-set", "b", 1);
        transaction.RemoveFromSet("expiring-set", "a");
        transaction.InsertToList("expiring-list", "a");
        transaction.InsertToList("expiring-list", "b");
        transaction.RemoveFromList("expiring-list", "a");
        transaction.TrimList("expiring-list", 0, 0);
        transaction.SetRangeInHash("expiring-hash", new Dictionary<string, string> { ["one"] = "1", ["two"] = "2" });
        transaction.ExpireSet("expiring-set", TimeSpan.FromSeconds(-1));
        transaction.ExpireHash("expiring-hash", TimeSpan.FromSeconds(-1));
        transaction.ExpireList("expiring-list", TimeSpan.FromSeconds(-1));
        transaction.Commit();

        Assert.Equal(0, connection.GetCounter("balanced-counter"));
        Assert.Equal("b", Assert.Single(connection.GetRangeFromSet("expiring-set", 0, 10)));
        Assert.Equal("b", Assert.Single(connection.GetRangeFromList("expiring-list", 0, 10)));
        Assert.Equal(2, connection.GetHashCount("expiring-hash"));
        Assert.Equal(2, connection.GetAllEntriesFromHash("expiring-hash").Count);
        Assert.True(connection.GetSetTtl("expiring-set") <= TimeSpan.Zero);
        Assert.True(connection.GetHashTtl("expiring-hash") <= TimeSpan.Zero);
        Assert.True(connection.GetListTtl("expiring-list") <= TimeSpan.Zero);

        var cleanup = new ExpiredRecordsCleanupProcess(storage, TimeSpan.FromMilliseconds(1), 100);
        cleanup.Execute(CancellationToken.None);
        Assert.Null(connection.GetJobData(expiringJobId));
        Assert.Equal(0, connection.GetSetCount("expiring-set"));
        Assert.Equal(0, connection.GetHashCount("expiring-hash"));
        Assert.Equal(0, connection.GetListCount("expiring-list"));

        using var persist = new OracleWriteOnlyTransaction(storage);
        persist.AddToSet("persist-set", "value");
        persist.InsertToList("persist-list", "value");
        persist.SetRangeInHash("persist-hash", new Dictionary<string, string> { ["field"] = "value" });
        persist.PersistJob(expiringJobId);
        persist.PersistSet("persist-set");
        persist.PersistHash("persist-hash");
        persist.PersistList("persist-list");
        persist.RemoveHash("persist-hash");
        persist.Commit();
    }

    private static void ExerciseServersAndMonitoring(
        OracleStorage storage,
        OracleStorageConnection connection,
        Job job)
    {
        connection.AnnounceServer("integration-server", new ServerContext
        {
            WorkerCount = 2,
            Queues = _defaultQueue
        });
        connection.Heartbeat("integration-server");
        Assert.Contains(storage.GetMonitoringApi().Servers(), server => server.Name == "integration-server");

        foreach (var stateName in new[] { "Scheduled", "Processing", "Succeeded", "Failed", "Deleted" })
        {
            var jobId = connection.CreateExpiredJob(job, new Dictionary<string, string>(), DateTime.UtcNow, TimeSpan.FromHours(1));
            using var transaction = connection.CreateWriteTransaction();
            transaction.SetJobState(jobId, CreateState(stateName));
            transaction.Commit();
            Assert.NotNull(connection.GetStateData(jobId));
        }

        var monitoring = storage.GetMonitoringApi();
        Assert.Single(monitoring.ScheduledJobs(0, 10));
        Assert.Single(monitoring.ProcessingJobs(0, 10));
        Assert.Single(monitoring.SucceededJobs(0, 10));
        Assert.Single(monitoring.FailedJobs(0, 10));
        Assert.Single(monitoring.DeletedJobs(0, 10));
        Assert.Equal(1, monitoring.ScheduledCount());
        Assert.Equal(1, monitoring.ProcessingCount());
        Assert.Equal(1, monitoring.SucceededListCount());
        Assert.Equal(1, monitoring.FailedCount());
        Assert.Equal(1, monitoring.DeletedListCount());
        Assert.Equal(8, monitoring.SucceededByDatesCount().Count);
        Assert.Equal(8, monitoring.FailedByDatesCount().Count);
        Assert.NotEmpty(monitoring.HourlySucceededJobs());
        Assert.NotEmpty(monitoring.HourlyFailedJobs());
        Assert.True(monitoring.GetStatistics().Servers >= 1);

        connection.RemoveServer("integration-server");
        Assert.DoesNotContain(storage.GetMonitoringApi().Servers(), server => server.Name == "integration-server");
        Assert.Equal(0, connection.RemoveTimedOutServers(TimeSpan.FromMinutes(1)));
    }

    private static IState CreateState(string name)
    {
        var timestamp = JobHelper.SerializeDateTime(DateTime.UtcNow);
        var data = new Dictionary<string, string>
        {
            ["ScheduledAt"] = timestamp,
            ["EnqueueAt"] = timestamp,
            ["StartedAt"] = timestamp,
            ["SucceededAt"] = timestamp,
            ["FailedAt"] = timestamp,
            ["DeletedAt"] = timestamp,
            ["ServerId"] = "integration-server",
            ["PerformanceDuration"] = "1",
            ["Latency"] = "2",
            ["ExceptionDetails"] = "details",
            ["ExceptionMessage"] = "message",
            ["ExceptionType"] = "type",
            ["Result"] = "result"
        };
        return new TestState(name, data);
    }

    private static void ExerciseMigration(OracleConnection connection, string prefix)
    {
        var originalJobCount = connection.ExecuteScalar<long>($"SELECT COUNT(*) FROM {prefix}JOB");
        connection.Execute($"DELETE FROM {prefix}HASH WHERE KEY_NAME = 'schema:version'");
        connection.Execute($"DROP INDEX IX_{prefix}DIST_LOCK_EXPIRE");
        connection.Execute($"ALTER TABLE {prefix}DISTRIBUTED_LOCK DROP COLUMN OWNER_ID");
        connection.Execute($"ALTER TABLE {prefix}DISTRIBUTED_LOCK DROP COLUMN EXPIRE_AT");

        OracleSchemaManager.EnsureSchemaCreated(connection, prefix);

        Assert.Equal(2, OracleSchemaManager.GetSchemaVersion(connection, prefix));
        Assert.Equal(originalJobCount, connection.ExecuteScalar<long>($"SELECT COUNT(*) FROM {prefix}JOB"));
    }
}

internal sealed class TestState(string name, Dictionary<string, string> data) : IState
{
    public string Name { get; } = name;
    public string Reason => "integration";
    public bool IsFinal => false;
    public bool IgnoreJobLoadException => false;
    public Dictionary<string, string> SerializeData() => data;
}

public static class OracleIntegrationJob
{
    private static TaskCompletionSource<bool> _completion = CreateCompletion();

    public static void Reset() => Volatile.Write(ref _completion, CreateCompletion());

    public static void Execute()
    {
        Volatile.Read(ref _completion).TrySetResult(true);
    }

    public static Task WaitAsync(TimeSpan timeout) => Volatile.Read(ref _completion).Task.WaitAsync(timeout);

    private static TaskCompletionSource<bool> CreateCompletion() =>
        new(TaskCreationOptions.RunContinuationsAsynchronously);
}
