using Dapper;
using Hangfire.Common;
using Hangfire.Storage;
using Hangfire.Storage.Monitoring;

namespace Hangfire.Oracle.Core;

/// <summary>
/// Oracle monitoring API implementation
/// </summary>
public class OracleMonitoringApi : IMonitoringApi
{
    private readonly OracleStorage _storage;

    /// <inheritdoc/>
    public OracleMonitoringApi(OracleStorage storage)
    {
        _storage = storage ?? throw new ArgumentNullException(nameof(storage));
    }

    /// <inheritdoc/>
    public StatisticsDto GetStatistics()
    {
        using var connection = _storage.CreateAndOpenConnection();

        var stats = new StatisticsDto();

        var counters = connection.Query<(string Key, long Value)>(
            $@"SELECT KEY_NAME COUNTER_KEY, SUM(VALUE) TOTAL_VALUE
               FROM (
                 SELECT KEY_NAME, VALUE FROM {_storage.GetTableName("COUNTER")}
                 UNION ALL
                 SELECT KEY_NAME, VALUE FROM {_storage.GetTableName("AGGREGATED_COUNTER")}
               )
               GROUP BY KEY_NAME",
            commandTimeout: _storage.Options.CommandTimeout);

        foreach (var counter in counters)
        {
            if (counter.Key == "stats:succeeded")
            {
                stats.Succeeded = counter.Value;
            }
            else if (counter.Key == "stats:deleted")
            {
                stats.Deleted = counter.Value;
            }
        }

        stats.Enqueued = connection.ExecuteScalar<long>(
            $@"SELECT COUNT(*) FROM {_storage.GetTableName("JOB")}
               WHERE STATE_NAME = 'Enqueued'",
            commandTimeout: _storage.Options.CommandTimeout);

        stats.Failed = connection.ExecuteScalar<long>(
            $@"SELECT COUNT(*) FROM {_storage.GetTableName("JOB")}
               WHERE STATE_NAME = 'Failed'",
            commandTimeout: _storage.Options.CommandTimeout);

        stats.Processing = connection.ExecuteScalar<long>(
            $@"SELECT COUNT(*) FROM {_storage.GetTableName("JOB")}
               WHERE STATE_NAME = 'Processing'",
            commandTimeout: _storage.Options.CommandTimeout);

        stats.Scheduled = connection.ExecuteScalar<long>(
            $@"SELECT COUNT(*) FROM {_storage.GetTableName("JOB")}
               WHERE STATE_NAME = 'Scheduled'",
            commandTimeout: _storage.Options.CommandTimeout);

        stats.Servers = connection.ExecuteScalar<long>(
            $@"SELECT COUNT(*) FROM {_storage.GetTableName("SERVER")}",
            commandTimeout: _storage.Options.CommandTimeout);

        stats.Queues = connection.ExecuteScalar<long>(
            $@"SELECT COUNT(DISTINCT QUEUE) FROM {_storage.GetTableName("JOB_QUEUE")}",
            commandTimeout: _storage.Options.CommandTimeout);

        stats.Recurring = connection.ExecuteScalar<long>(
            $@"SELECT COUNT(*) FROM {_storage.GetTableName("SET")}
               WHERE KEY_NAME = 'recurring-jobs'",
            commandTimeout: _storage.Options.CommandTimeout);

        return stats;
    }

    /// <inheritdoc/>
    public JobList<EnqueuedJobDto> EnqueuedJobs(string queue, int from, int perPage)
    {
        return GetJobsOnQueue(queue, from, perPage, "Enqueued");
    }

    /// <inheritdoc/>
    public JobList<FetchedJobDto> FetchedJobs(string queue, int from, int perPage)
    {
        return GetJobsOnQueue<FetchedJobDto>(queue, from, perPage, "Fetched");
    }

    /// <inheritdoc/>
    public JobList<ProcessingJobDto> ProcessingJobs(int from, int count)
    {
        return GetJobs<ProcessingJobDto>(from, count, "Processing",
            (job, state) =>
            {
                state.Data.TryGetValue("ServerId", out var serverId);

                return new ProcessingJobDto
                {
                    Job = job,
                    ServerId = serverId,
                    StartedAt = GetDateTime(state.Data, "StartedAt"),
                    InProcessingState = string.Equals(state.Name, "Processing", StringComparison.Ordinal),
                    StateData = state.Data
                };
            });
    }

    /// <inheritdoc/>
    public JobList<ScheduledJobDto> ScheduledJobs(int from, int count)
    {
        return GetJobs<ScheduledJobDto>(from, count, "Scheduled",
            (job, state) =>
            {
                return new ScheduledJobDto
                {
                    Job = job,
                    EnqueueAt = GetDateTime(state.Data, "EnqueueAt") ?? DateTime.MinValue,
                    ScheduledAt = GetDateTime(state.Data, "ScheduledAt") ?? GetDateTime(state.Data, "EnqueueAt"),
                    InScheduledState = string.Equals(state.Name, "Scheduled", StringComparison.Ordinal),
                    StateData = state.Data
                };
            });
    }

    /// <inheritdoc/>
    public JobList<SucceededJobDto> SucceededJobs(int from, int count)
    {
        return GetJobs<SucceededJobDto>(from, count, "Succeeded",
            (job, state) =>
            {
                state.Data.TryGetValue("Result", out var result);

                long? totalDuration = null;
                if (state.Data.TryGetValue("PerformanceDuration", out var performanceDurationString) &&
                    state.Data.TryGetValue("Latency", out var latencyString))
                {
                    totalDuration = long.Parse(performanceDurationString) + long.Parse(latencyString);
                }

                return new SucceededJobDto
                {
                    Job = job,
                    Result = result,
                    TotalDuration = totalDuration,
                    SucceededAt = GetDateTime(state.Data, "SucceededAt"),
                    InSucceededState = string.Equals(state.Name, "Succeeded", StringComparison.Ordinal),
                    StateData = state.Data
                };
            });
    }

    /// <inheritdoc/>
    public JobList<FailedJobDto> FailedJobs(int from, int count)
    {
        return GetJobs<FailedJobDto>(from, count, "Failed",
            (job, state) =>
            {
                state.Data.TryGetValue("ExceptionDetails", out var exceptionDetails);
                state.Data.TryGetValue("ExceptionMessage", out var exceptionMessage);
                state.Data.TryGetValue("ExceptionType", out var exceptionType);
                return new FailedJobDto
                {
                    Job = job,
                    Reason = state.Reason,
                    ExceptionDetails = exceptionDetails,
                    ExceptionMessage = exceptionMessage,
                    ExceptionType = exceptionType,
                    FailedAt = GetDateTime(state.Data, "FailedAt"),
                    InFailedState = string.Equals(state.Name, "Failed", StringComparison.Ordinal),
                    StateData = state.Data
                };
            });
    }

    /// <inheritdoc/>
    public JobList<DeletedJobDto> DeletedJobs(int from, int count)
    {
        return GetJobs<DeletedJobDto>(from, count, "Deleted",
            (job, state) =>
            {
                return new DeletedJobDto
                {
                    Job = job,
                    DeletedAt = GetDateTime(state.Data, "DeletedAt"),
                    InDeletedState = string.Equals(state.Name, "Deleted", StringComparison.Ordinal),
                    StateData = state.Data
                };
            });
    }

    /// <inheritdoc/>
    public long EnqueuedCount(string queue)
    {
        using var connection = _storage.CreateAndOpenConnection();

        return connection.ExecuteScalar<long>(
            $@"SELECT COUNT(*) FROM {_storage.GetTableName("JOB_QUEUE")}
               WHERE QUEUE = :queue AND FETCHED_AT IS NULL",
            new { queue },
            commandTimeout: _storage.Options.CommandTimeout);
    }

    /// <inheritdoc/>
    public long FetchedCount(string queue)
    {
        using var connection = _storage.CreateAndOpenConnection();

        return connection.ExecuteScalar<long>(
            $@"SELECT COUNT(*) FROM {_storage.GetTableName("JOB_QUEUE")}
               WHERE QUEUE = :queue AND FETCHED_AT IS NOT NULL",
            new { queue },
            commandTimeout: _storage.Options.CommandTimeout);
    }

    /// <inheritdoc/>
    public long ScheduledCount()
    {
        return GetCountByState("Scheduled");
    }

    /// <inheritdoc/>
    public long ProcessingCount()
    {
        return GetCountByState("Processing");
    }

    /// <inheritdoc/>
    public long SucceededListCount()
    {
        return GetCountByState("Succeeded");
    }

    /// <inheritdoc/>
    public long FailedCount()
    {
        return GetCountByState("Failed");
    }

    /// <inheritdoc/>
    public long DeletedListCount()
    {
        return GetCountByState("Deleted");
    }

    /// <inheritdoc/>
    public IDictionary<DateTime, long> SucceededByDatesCount()
    {
        return GetTimelineStats("Succeeded");
    }

    /// <inheritdoc/>
    public IDictionary<DateTime, long> FailedByDatesCount()
    {
        return GetTimelineStats("Failed");
    }

    /// <inheritdoc/>
    public IDictionary<DateTime, long> HourlySucceededJobs()
    {
        return GetHourlyTimelineStats("Succeeded");
    }

    /// <inheritdoc/>
    public IDictionary<DateTime, long> HourlyFailedJobs()
    {
        return GetHourlyTimelineStats("Failed");
    }

    /// <inheritdoc/>
    public IList<ServerDto> Servers()
    {
        using var connection = _storage.CreateAndOpenConnection();

        var servers = connection.Query(
            $@"SELECT ID, DATA, LAST_HEARTBEAT
               FROM {_storage.GetTableName("SERVER")}",
            commandTimeout: _storage.Options.CommandTimeout);

        return servers.Select(x =>
        {
            var serverData = JobHelper.FromJson<ServerData>(x.DATA);

            return new ServerDto
            {
                Name = x.ID,
                Heartbeat = x.LAST_HEARTBEAT,
                Queues = serverData?.Queues ?? Array.Empty<string>(),
                StartedAt = serverData?.StartedAt ?? DateTime.MinValue,
                WorkersCount = serverData?.WorkerCount ?? 0
            };
        }).ToList();
    }

    // Helper class to deserialize server data
    private class ServerData
    {
        public int WorkerCount { get; set; }
        public string[] Queues { get; set; } = Array.Empty<string>();
        public DateTime StartedAt { get; set; }
    }

    /// <inheritdoc/>
    public IList<QueueWithTopEnqueuedJobsDto> Queues()
    {
        using var connection = _storage.CreateAndOpenConnection();

        var queues = connection.Query<string>(
            $@"SELECT DISTINCT QUEUE FROM {_storage.GetTableName("JOB_QUEUE")}",
            commandTimeout: _storage.Options.CommandTimeout)
            .ToList();

        return queues.Select(queue => new QueueWithTopEnqueuedJobsDto
        {
            Name = queue,
            Length = EnqueuedCount(queue),
            Fetched = FetchedCount(queue),
            FirstJobs = EnqueuedJobs(queue, 0, 5)
        }).ToList();
    }

    /// <inheritdoc/>
    public JobDetailsDto? JobDetails(string jobId)
    {
        using var connection = _storage.CreateAndOpenConnection();

        var job = connection.Query(
            $@"SELECT INVOCATION_DATA, ARGUMENTS, CREATED_AT, EXPIRE_AT, STATE_NAME
               FROM {_storage.GetTableName("JOB")}
               WHERE ID = :id",
            new { id = long.Parse(jobId) },
            commandTimeout: _storage.Options.CommandTimeout)
            .SingleOrDefault();

        if (job == null)
        {
            return null;
        }

        var history = connection.Query(
            $@"SELECT NAME, REASON, CREATED_AT, DATA
               FROM {_storage.GetTableName("JOB_STATE")}
               WHERE JOB_ID = :jobId
               ORDER BY CREATED_AT DESC",
            new { jobId = long.Parse(jobId) },
            commandTimeout: _storage.Options.CommandTimeout)
            .ToList();

        var invocationData = JobHelper.FromJson<InvocationData>(job.INVOCATION_DATA);
        invocationData.Arguments = job.ARGUMENTS;

        return new JobDetailsDto
        {
            CreatedAt = job.CREATED_AT,
            ExpireAt = job.EXPIRE_AT,
            Job = invocationData.DeserializeJob(),
            History = history.Select(x => new StateHistoryDto
            {
                StateName = x.NAME,
                Reason = x.REASON,
                CreatedAt = x.CREATED_AT,
                Data = JobHelper.FromJson<Dictionary<string, string>>(x.DATA) ?? new Dictionary<string, string>()
            }).ToList(),
            Properties = new Dictionary<string, string>()
        };
    }

    private long GetCountByState(string stateName)
    {
        using var connection = _storage.CreateAndOpenConnection();

        return connection.ExecuteScalar<long>(
            $@"SELECT COUNT(*) FROM {_storage.GetTableName("JOB")}
               WHERE STATE_NAME = :state",
            new { state = stateName },
            commandTimeout: _storage.Options.CommandTimeout);
    }

    private Dictionary<DateTime, long> GetTimelineStats(string type)
    {
        var endDate = _storage.GetUtcOrLocalNow().Date;
        var startDate = endDate.AddDays(-7);
        var dates = new Dictionary<DateTime, long>();

        for (var date = startDate; date <= endDate; date = date.AddDays(1))
        {
            dates[date] = 0;
        }

        using var connection = _storage.CreateAndOpenConnection();

        var counters = connection.Query<(DateTime Date, long Count)>(
            $@"SELECT TRUNC(CREATED_AT) STATE_DATE, COUNT(*) TOTAL_COUNT
               FROM {_storage.GetTableName("JOB_STATE")}
               WHERE NAME = :stateName
                 AND CREATED_AT >= :startDate
               GROUP BY TRUNC(CREATED_AT)",
            new { stateName = type, startDate },
            commandTimeout: _storage.Options.CommandTimeout);

        foreach (var counter in counters)
        {
            dates[counter.Date] = counter.Count;
        }

        return dates;
    }

    private Dictionary<DateTime, long> GetHourlyTimelineStats(string type)
    {
        var endDate = _storage.GetUtcOrLocalNow();
        var startDate = endDate.AddHours(-24);
        var hours = new Dictionary<DateTime, long>();

        for (var date = startDate; date <= endDate; date = date.AddHours(1))
        {
            hours[date] = 0;
        }

        using var connection = _storage.CreateAndOpenConnection();

        var counters = connection.Query<(DateTime Date, long Count)>(
            $@"SELECT TRUNC(CREATED_AT, 'HH24') STATE_DATE, COUNT(*) TOTAL_COUNT
               FROM {_storage.GetTableName("JOB_STATE")}
               WHERE NAME = :stateName
                 AND CREATED_AT >= :startDate
               GROUP BY TRUNC(CREATED_AT, 'HH24')",
            new { stateName = type, startDate },
            commandTimeout: _storage.Options.CommandTimeout);

        foreach (var counter in counters)
        {
            hours[counter.Date] = counter.Count;
        }

        return hours;
    }

    private JobList<EnqueuedJobDto> GetJobsOnQueue(string queue, int from, int perPage, string stateName)
    {
        return GetJobsOnQueue<EnqueuedJobDto>(queue, from, perPage, stateName);
    }

    private JobList<T> GetJobsOnQueue<T>(string queue, int from, int perPage, string stateName)
        where T : new()
    {
        (from, perPage) = NormalizePage(from, perPage);
        using var connection = _storage.CreateAndOpenConnection();

        var jobs = connection.Query(
            $@"SELECT * FROM (
                 SELECT j.ID, j.INVOCATION_DATA, j.ARGUMENTS, j.CREATED_AT, j.EXPIRE_AT, j.STATE_NAME,
                        jq.FETCHED_AT, ROW_NUMBER() OVER (ORDER BY jq.ID) AS RN
                 FROM {_storage.GetTableName("JOB_QUEUE")} jq
                 INNER JOIN {_storage.GetTableName("JOB")} j ON jq.JOB_ID = j.ID
                 WHERE jq.QUEUE = :queue
               )
               WHERE RN > :offsetRow AND RN <= :endRow",
            new { queue, offsetRow = from, endRow = from + perPage },
            commandTimeout: _storage.Options.CommandTimeout)
            .ToList();

        return new JobList<T>(jobs.Select(job =>
        {
            var invocationData = JobHelper.FromJson<InvocationData>(job.INVOCATION_DATA);
            invocationData.Arguments = job.ARGUMENTS;

            var deserializedJob = default(Job);
            try
            {
                deserializedJob = invocationData.DeserializeJob();
            }
            catch
            {
                // Job deserialization failed
            }

            object dto;

            if (typeof(T) == typeof(EnqueuedJobDto))
            {
                var enqueuedDto = new EnqueuedJobDto
                {
                    Job = deserializedJob,
                    State = job.STATE_NAME,
                    InEnqueuedState = string.Equals(job.STATE_NAME, stateName, StringComparison.OrdinalIgnoreCase),
                    EnqueuedAt = job.CREATED_AT,
                    InvocationData = invocationData
                };

                dto = enqueuedDto;
            }
            else if (typeof(T) == typeof(FetchedJobDto))
            {
                dto = new FetchedJobDto
                {
                    Job = deserializedJob,
                    InvocationData = invocationData,
                    State = job.STATE_NAME,
                    FetchedAt = job.FETCHED_AT
                };
            }
            else
            {
                dto = new T();
            }

            return new KeyValuePair<string, T>(
                job.ID.ToString(),
                (T)dto
            );
        }).ToList());
    }

    private JobList<T> GetJobs<T>(int from, int count, string stateName,
        Func<Job, StateData, T> selector)
    {
        (from, count) = NormalizePage(from, count);
        using var connection = _storage.CreateAndOpenConnection();

        var jobs = connection.Query(
            $@"SELECT * FROM (
                 SELECT j.ID, j.INVOCATION_DATA, j.ARGUMENTS, j.CREATED_AT, j.STATE_ID,
                        s.NAME, s.REASON, s.DATA,
                        ROW_NUMBER() OVER (ORDER BY j.ID DESC) AS RN
                 FROM {_storage.GetTableName("JOB")} j
                 LEFT JOIN {_storage.GetTableName("JOB_STATE")} s ON j.STATE_ID = s.ID
                 WHERE j.STATE_NAME = :stateName
               )
               WHERE RN > :offsetRow AND RN <= :endRow",
            new { stateName, offsetRow = from, endRow = from + count },
            commandTimeout: _storage.Options.CommandTimeout)
            .ToList();

        return new JobList<T>(jobs.Select(job =>
        {
            var invocationData = JobHelper.FromJson<InvocationData>(job.INVOCATION_DATA);
            invocationData.Arguments = job.ARGUMENTS;

            var deserializedJob = default(Job);
            try
            {
                deserializedJob = invocationData.DeserializeJob();
            }
            catch
            {
                // Job deserialization failed
            }

            var stateData = new StateData
            {
                Name = job.NAME,
                Reason = job.REASON,
                Data = JobHelper.FromJson<Dictionary<string, string>>(job.DATA) ?? new Dictionary<string, string>()
            };

            return new KeyValuePair<string, T>(
                job.ID.ToString(),
                selector(deserializedJob!, stateData)
            );
        }).ToList());
    }

    private (int From, int Count) NormalizePage(int from, int count)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(from);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(count);
        return (from, Math.Min(count, _storage.Options.DashboardJobListLimit));
    }

    private static DateTime? GetDateTime(IDictionary<string, string> data, string key)
    {
        return data.TryGetValue(key, out var value) && !string.IsNullOrWhiteSpace(value)
            ? JobHelper.DeserializeDateTime(value)
            : null;
    }
}
