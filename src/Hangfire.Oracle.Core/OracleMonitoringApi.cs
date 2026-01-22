using System;
using System.Collections.Generic;
using System.Linq;
using Dapper;
using Hangfire.Common;
using Hangfire.States;
using Hangfire.Storage;
using Hangfire.Storage.Monitoring;

namespace Hangfire.Oracle.Core;

/// <summary>
/// Oracle monitoring API implementation
/// </summary>
public class OracleMonitoringApi : IMonitoringApi
{
    private readonly OracleStorage _storage;

    public OracleMonitoringApi(OracleStorage storage)
    {
        _storage = storage ?? throw new ArgumentNullException(nameof(storage));
    }

    public StatisticsDto GetStatistics()
    {
        using var connection = _storage.CreateAndOpenConnection();

        var stats = new StatisticsDto();

        var counters = connection.Query<(string Key, long Value)>(
            $@"SELECT KEY_NAME AS Key, SUM(VALUE) AS Value 
               FROM {_storage.GetTableName("COUNTER")}
               GROUP BY KEY_NAME");

        foreach (var counter in counters)
        {
            if (counter.Key == "stats:succeeded")
                stats.Succeeded = counter.Value;
            else if (counter.Key == "stats:deleted")
                stats.Deleted = counter.Value;
        }

        stats.Enqueued = connection.ExecuteScalar<long>(
            $@"SELECT COUNT(*) FROM {_storage.GetTableName("JOB")}
               WHERE STATE_NAME = 'Enqueued'");

        stats.Failed = connection.ExecuteScalar<long>(
            $@"SELECT COUNT(*) FROM {_storage.GetTableName("JOB")}
               WHERE STATE_NAME = 'Failed'");

        stats.Processing = connection.ExecuteScalar<long>(
            $@"SELECT COUNT(*) FROM {_storage.GetTableName("JOB")}
               WHERE STATE_NAME = 'Processing'");

        stats.Scheduled = connection.ExecuteScalar<long>(
            $@"SELECT COUNT(*) FROM {_storage.GetTableName("JOB")}
               WHERE STATE_NAME = 'Scheduled'");

        stats.Servers = connection.ExecuteScalar<long>(
            $@"SELECT COUNT(*) FROM {_storage.GetTableName("SERVER")}");

        stats.Queues = connection.ExecuteScalar<long>(
            $@"SELECT COUNT(DISTINCT QUEUE) FROM {_storage.GetTableName("JOB_QUEUE")}");

        stats.Recurring = connection.ExecuteScalar<long>(
            $@"SELECT COUNT(*) FROM {_storage.GetTableName("SET")}
               WHERE KEY_NAME = 'recurring-jobs'");

        return stats;
    }

    public JobList<EnqueuedJobDto> EnqueuedJobs(string queue, int from, int perPage)
    {
        return GetJobsOnQueue(queue, from, perPage, "Enqueued");
    }

    public JobList<FetchedJobDto> FetchedJobs(string queue, int from, int perPage)
    {
        return GetJobsOnQueue<FetchedJobDto>(queue, from, perPage, "Fetched");
    }

    public JobList<ProcessingJobDto> ProcessingJobs(int from, int count)
    {
        return GetJobs<ProcessingJobDto>(from, count, "Processing", 
            (job, state) => new ProcessingJobDto
            {
                Job = job,
                ServerId = state.Data.ContainsKey("ServerId") ? state.Data["ServerId"] : null,
                StartedAt = JobHelper.DeserializeDateTime(state.Data["StartedAt"])
            });
    }

    public JobList<ScheduledJobDto> ScheduledJobs(int from, int count)
    {
        return GetJobs<ScheduledJobDto>(from, count, "Scheduled",
            (job, state) => new ScheduledJobDto
            {
                Job = job,
                EnqueueAt = JobHelper.DeserializeDateTime(state.Data["EnqueueAt"]),
                ScheduledAt = JobHelper.DeserializeDateTime(state.Data.ContainsKey("ScheduledAt") 
                    ? state.Data["ScheduledAt"] 
                    : state.Data["EnqueueAt"])
            });
    }

    public JobList<SucceededJobDto> SucceededJobs(int from, int count)
    {
        return GetJobs<SucceededJobDto>(from, count, "Succeeded",
            (job, state) => new SucceededJobDto
            {
                Job = job,
                Result = state.Data.ContainsKey("Result") ? state.Data["Result"] : null,
                TotalDuration = state.Data.ContainsKey("PerformanceDuration") && state.Data.ContainsKey("Latency")
                    ? (long?)long.Parse(state.Data["PerformanceDuration"]) + long.Parse(state.Data["Latency"])
                    : null,
                SucceededAt = JobHelper.DeserializeDateTime(state.Data["SucceededAt"])
            });
    }

    public JobList<FailedJobDto> FailedJobs(int from, int count)
    {
        return GetJobs<FailedJobDto>(from, count, "Failed",
            (job, state) => new FailedJobDto
            {
                Job = job,
                Reason = state.Reason,
                ExceptionDetails = state.Data["ExceptionDetails"],
                ExceptionMessage = state.Data["ExceptionMessage"],
                ExceptionType = state.Data["ExceptionType"],
                FailedAt = JobHelper.DeserializeDateTime(state.Data["FailedAt"])
            });
    }

    public JobList<DeletedJobDto> DeletedJobs(int from, int count)
    {
        return GetJobs<DeletedJobDto>(from, count, "Deleted",
            (job, state) => new DeletedJobDto
            {
                Job = job,
                DeletedAt = JobHelper.DeserializeDateTime(state.Data.ContainsKey("DeletedAt") 
                    ? state.Data["DeletedAt"] 
                    : null)
            });
    }

    public long EnqueuedCount(string queue)
    {
        using var connection = _storage.CreateAndOpenConnection();

        return connection.ExecuteScalar<long>(
            $@"SELECT COUNT(*) FROM {_storage.GetTableName("JOB_QUEUE")}
               WHERE QUEUE = :queue AND FETCHED_AT IS NULL",
            new { queue });
    }

    public long FetchedCount(string queue)
    {
        using var connection = _storage.CreateAndOpenConnection();

        return connection.ExecuteScalar<long>(
            $@"SELECT COUNT(*) FROM {_storage.GetTableName("JOB_QUEUE")}
               WHERE QUEUE = :queue AND FETCHED_AT IS NOT NULL",
            new { queue });
    }

    public long ScheduledCount()
    {
        return GetCountByState("Scheduled");
    }

    public long ProcessingCount()
    {
        return GetCountByState("Processing");
    }

    public long SucceededListCount()
    {
        return GetCountByState("Succeeded");
    }

    public long FailedCount()
    {
        return GetCountByState("Failed");
    }

    public long DeletedListCount()
    {
        return GetCountByState("Deleted");
    }

    public IDictionary<DateTime, long> SucceededByDatesCount()
    {
        return GetTimelineStats("succeeded");
    }

    public IDictionary<DateTime, long> FailedByDatesCount()
    {
        return GetTimelineStats("failed");
    }

    public IDictionary<DateTime, long> HourlySucceededJobs()
    {
        return GetHourlyTimelineStats("succeeded");
    }

    public IDictionary<DateTime, long> HourlyFailedJobs()
    {
        return GetHourlyTimelineStats("failed");
    }

    public IList<ServerDto> Servers()
    {
        using var connection = _storage.CreateAndOpenConnection();

        var servers = connection.Query(
            $@"SELECT ID, DATA, LAST_HEARTBEAT
               FROM {_storage.GetTableName("SERVER")}");

        return servers.Select(x => new ServerDto
        {
            Name = x.ID,
            Heartbeat = x.LAST_HEARTBEAT,
            Queues = JobHelper.FromJson<string[]>(x.DATA) ?? Array.Empty<string>(),
            StartedAt = DateTime.MinValue,
            WorkersCount = 0
        }).ToList();
    }

    public IList<QueueWithTopEnqueuedJobsDto> Queues()
    {
        using var connection = _storage.CreateAndOpenConnection();

        var queues = connection.Query<string>(
            $@"SELECT DISTINCT QUEUE FROM {_storage.GetTableName("JOB_QUEUE")}")
            .ToList();

        return queues.Select(queue => new QueueWithTopEnqueuedJobsDto
        {
            Name = queue,
            Length = EnqueuedCount(queue),
            Fetched = FetchedCount(queue),
            FirstJobs = EnqueuedJobs(queue, 0, 5)
        }).ToList();
    }

    public JobDetailsDto JobDetails(string jobId)
    {
        using var connection = _storage.CreateAndOpenConnection();

        var job = connection.Query(
            $@"SELECT INVOCATION_DATA, ARGUMENTS, CREATED_AT, EXPIRE_AT, STATE_NAME
               FROM {_storage.GetTableName("JOB")}
               WHERE ID = :id",
            new { id = long.Parse(jobId) })
            .SingleOrDefault();

        if (job == null)
            return null;

        var history = connection.Query(
            $@"SELECT NAME, REASON, CREATED_AT, DATA
               FROM {_storage.GetTableName("JOB_STATE")}
               WHERE JOB_ID = :jobId
               ORDER BY CREATED_AT DESC",
            new { jobId = long.Parse(jobId) })
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
            new { state = stateName });
    }

    private Dictionary<DateTime, long> GetTimelineStats(string type)
    {
        var endDate = DateTime.UtcNow.Date;
        var startDate = endDate.AddDays(-7);
        var dates = new Dictionary<DateTime, long>();

        for (var date = startDate; date <= endDate; date = date.AddDays(1))
        {
            dates[date] = 0;
        }

        using var connection = _storage.CreateAndOpenConnection();

        var counters = connection.Query<(DateTime Date, long Count)>(
            $@"SELECT TRUNC(CREATED_AT) AS Date, COUNT(*) AS Count
               FROM {_storage.GetTableName("JOB_STATE")}
               WHERE NAME = :stateName
                 AND CREATED_AT >= :startDate
               GROUP BY TRUNC(CREATED_AT)",
            new { stateName = type, startDate });

        foreach (var counter in counters)
        {
            dates[counter.Date] = counter.Count;
        }

        return dates;
    }

    private Dictionary<DateTime, long> GetHourlyTimelineStats(string type)
    {
        var endDate = DateTime.UtcNow;
        var startDate = endDate.AddHours(-24);
        var hours = new Dictionary<DateTime, long>();

        for (var date = startDate; date <= endDate; date = date.AddHours(1))
        {
            hours[date] = 0;
        }

        using var connection = _storage.CreateAndOpenConnection();

        var counters = connection.Query<(DateTime Date, long Count)>(
            $@"SELECT TRUNC(CREATED_AT, 'HH') AS Date, COUNT(*) AS Count
               FROM {_storage.GetTableName("JOB_STATE")}
               WHERE NAME = :stateName
                 AND CREATED_AT >= :startDate
               GROUP BY TRUNC(CREATED_AT, 'HH')",
            new { stateName = type, startDate });

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
        using var connection = _storage.CreateAndOpenConnection();

        var jobs = connection.Query(
            $@"SELECT * FROM (
                 SELECT j.ID, j.INVOCATION_DATA, j.ARGUMENTS, j.CREATED_AT, j.EXPIRE_AT, j.STATE_NAME,
                        ROW_NUMBER() OVER (ORDER BY jq.ID) AS ROWNUM
                 FROM {_storage.GetTableName("JOB_QUEUE")} jq
                 INNER JOIN {_storage.GetTableName("JOB")} j ON jq.JOB_ID = j.ID
                 WHERE jq.QUEUE = :queue
               )
               WHERE ROWNUM > :from AND ROWNUM <= :to",
            new { queue, from, to = from + perPage })
            .ToList();

        return new JobList<T>(jobs.Select(job => 
        {
            var invocationData = JobHelper.FromJson<InvocationData>(job.INVOCATION_DATA);
            invocationData.Arguments = job.ARGUMENTS;

            return new KeyValuePair<string, T>(
                job.ID.ToString(),
                new T()
            );
        }).ToList());
    }

    private JobList<T> GetJobs<T>(int from, int count, string stateName, 
        Func<Job, StateData, T> selector)
    {
        using var connection = _storage.CreateAndOpenConnection();

        var jobs = connection.Query(
            $@"SELECT * FROM (
                 SELECT j.ID, j.INVOCATION_DATA, j.ARGUMENTS, j.CREATED_AT, j.STATE_ID,
                        s.NAME, s.REASON, s.DATA,
                        ROW_NUMBER() OVER (ORDER BY j.ID DESC) AS ROWNUM
                 FROM {_storage.GetTableName("JOB")} j
                 LEFT JOIN {_storage.GetTableName("JOB_STATE")} s ON j.STATE_ID = s.ID
                 WHERE j.STATE_NAME = :stateName
               )
               WHERE ROWNUM > :from AND ROWNUM <= :to",
            new { stateName, from, to = from + count })
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
                selector(deserializedJob, stateData)
            );
        }).ToList());
    }
}
