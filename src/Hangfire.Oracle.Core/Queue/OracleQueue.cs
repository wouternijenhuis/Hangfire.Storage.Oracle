using System.Data;
using Dapper;
using Hangfire.Logging;
using Hangfire.Storage;
using Oracle.ManagedDataAccess.Client;
using Oracle.ManagedDataAccess.Types;

namespace Hangfire.Oracle.Core.Queue;

/// <summary>
/// Fetches and enqueues jobs using a single ownership-aware Oracle implementation.
/// </summary>
internal sealed class OracleQueue : IJobQueue
{
    private static readonly ILog _logger = LogProvider.GetLogger(typeof(OracleQueue));
    private readonly OracleStorage _storage;
    private readonly OracleStorageOptions _options;

    public OracleQueue(OracleStorage storage, OracleStorageOptions options)
    {
        _storage = storage ?? throw new ArgumentNullException(nameof(storage));
        _options = options ?? throw new ArgumentNullException(nameof(options));
    }

    public IFetchedJob? Dequeue(string[] queues, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(queues);
        if (queues.Length == 0 || queues.Any(string.IsNullOrWhiteSpace))
        {
            throw new ArgumentException("At least one non-empty queue name must be specified.", nameof(queues));
        }

        for (var attempt = 0; attempt < _options.FetchCount; attempt++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var fetchedJob = TryFetchJob(queues);
            if (fetchedJob is not null)
            {
                return fetchedJob;
            }
        }

        return null;
    }

    public void Enqueue(IDbConnection connection, IDbTransaction? transaction, string queue, string jobId)
    {
        ArgumentNullException.ThrowIfNull(connection);
        if (string.IsNullOrWhiteSpace(queue))
        {
            throw new ArgumentException("Queue names cannot be empty.", nameof(queue));
        }

        if (!long.TryParse(jobId, out var numericJobId))
        {
            throw new ArgumentException("Job identifiers must be numeric.", nameof(jobId));
        }

        connection.Execute(
            $@"INSERT INTO {_storage.GetTableName("JOB_QUEUE")} (ID, JOB_ID, QUEUE, FETCHED_AT, FETCH_TOKEN)
               VALUES ({_storage.GetTableName("JOB_QUEUE_SEQ")}.NEXTVAL, :jobId, :queue, NULL, NULL)",
            new { jobId = numericJobId, queue },
            transaction,
            _options.CommandTimeout);
    }

    internal string BuildFetchBlock(string[] queues)
    {
        var queueParameters = string.Join(", ", queues.Select((_, index) => $":queue{index}"));
        var lockClause = _options.SupportsSkipLocked ? "FOR UPDATE SKIP LOCKED" : "FOR UPDATE NOWAIT";
        return $@"DECLARE
  CURSOR next_job IS
    SELECT ID, JOB_ID, QUEUE
    FROM {_storage.GetTableName("JOB_QUEUE")}
    WHERE QUEUE IN ({queueParameters})
      AND (FETCHED_AT IS NULL OR FETCHED_AT < :staleAt)
    ORDER BY ID
    {lockClause};
BEGIN
  :found := 0;
  OPEN next_job;
  FETCH next_job INTO :queueId, :jobId, :queueName;
  IF next_job%FOUND THEN
    UPDATE {_storage.GetTableName("JOB_QUEUE")}
    SET FETCHED_AT = :fetchedAt, FETCH_TOKEN = :fetchToken
    WHERE CURRENT OF next_job;
    :found := 1;
  END IF;
  CLOSE next_job;
END;";
    }

    private FetchedJobContext? TryFetchJob(string[] queues)
    {
        var fetchToken = Guid.NewGuid().ToString("N");
        var now = _storage.GetUtcOrLocalNow();

        using var connection = _storage.CreateAndOpenConnection();
        using var transaction = connection.BeginTransaction(_options.TransactionIsolationLevel);
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.BindByName = true;
        command.CommandTimeout = _options.CommandTimeout;
        command.CommandText = BuildFetchBlock(queues);

        for (var index = 0; index < queues.Length; index++)
        {
            command.Parameters.Add($"queue{index}", OracleDbType.NVarchar2, queues[index], ParameterDirection.Input);
        }

        command.Parameters.Add("staleAt", OracleDbType.TimeStamp, now.Subtract(_options.InvisibilityTimeout), ParameterDirection.Input);
        command.Parameters.Add("found", OracleDbType.Int32, ParameterDirection.Output);
        command.Parameters.Add("queueId", OracleDbType.Int64, ParameterDirection.Output);
        command.Parameters.Add("jobId", OracleDbType.Int64, ParameterDirection.Output);
        command.Parameters.Add("queueName", OracleDbType.NVarchar2, 50, null, ParameterDirection.Output);
        command.Parameters.Add("fetchedAt", OracleDbType.TimeStamp, now, ParameterDirection.Input);
        command.Parameters.Add("fetchToken", OracleDbType.NVarchar2, fetchToken, ParameterDirection.Input);

        try
        {
            command.ExecuteNonQuery();
            if (ConvertOracleInt64(command.Parameters["found"].Value) == 0)
            {
                transaction.Rollback();
                return null;
            }

            var queueId = ConvertOracleInt64(command.Parameters["queueId"].Value);
            var jobId = ConvertOracleInt64(command.Parameters["jobId"].Value);
            var queueName = command.Parameters["queueName"].Value.ToString()
                ?? throw new InvalidOperationException("Oracle returned an empty queue name.");
            transaction.Commit();

            _logger.TraceFormat("Dequeued job {0} from queue '{1}'.", jobId, queueName);
            return new FetchedJobContext(_storage, queueId, jobId, queueName, fetchToken);
        }
        catch (OracleException ex) when (!_options.SupportsSkipLocked && OracleErrorCodes.IsResourceBusy(ex))
        {
            TryRollback(transaction);
            return null;
        }
        catch
        {
            TryRollback(transaction);
            throw;
        }
    }

    private static void TryRollback(IDbTransaction transaction)
    {
        try
        {
            transaction.Rollback();
        }
        catch (InvalidOperationException)
        {
            // The transaction is already completed or the connection was lost.
        }
        catch (OracleException)
        {
            // Preserve the exception that caused the rollback.
        }
    }

    private static long ConvertOracleInt64(object value)
    {
        return value is OracleDecimal oracleDecimal
            ? decimal.ToInt64(oracleDecimal.Value)
            : Convert.ToInt64(value, System.Globalization.CultureInfo.InvariantCulture);
    }
}
