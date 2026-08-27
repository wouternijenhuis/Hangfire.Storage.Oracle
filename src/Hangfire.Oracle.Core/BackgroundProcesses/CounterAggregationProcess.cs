using Hangfire.Logging;
using Hangfire.Server;
using Oracle.ManagedDataAccess.Client;

namespace Hangfire.Oracle.Core.BackgroundProcesses;

/// <summary>
/// Background process that aggregates individual counter records into summarized counters.
/// This reduces the number of rows in the counter table and improves query performance.
/// </summary>
#pragma warning disable CS0618 // IServerComponent is obsolete but still required
internal sealed class CounterAggregationProcess : IServerComponent
#pragma warning restore CS0618
{
    private static readonly ILog _logger = LogProvider.GetLogger(typeof(CounterAggregationProcess));

    private readonly OracleStorage _storage;
    private readonly TimeSpan _aggregationInterval;
    private readonly int _batchSize;

    /// <summary>
    /// Creates a new counter aggregation process.
    /// </summary>
    /// <param name="storage">The Oracle storage instance.</param>
    /// <param name="aggregationInterval">How often to run aggregation.</param>
    /// <param name="batchSize">Maximum records to process per batch.</param>
    public CounterAggregationProcess(
        OracleStorage storage,
        TimeSpan aggregationInterval,
        int batchSize = 1000)
    {
        _storage = storage ?? throw new ArgumentNullException(nameof(storage));
        _aggregationInterval = aggregationInterval;
        _batchSize = batchSize;
    }

    /// <summary>
    /// Executes the counter aggregation process.
    /// </summary>
    public void Execute(CancellationToken cancellationToken)
    {
        _logger.Debug("Starting counter aggregation...");

        var totalAggregated = 0;
        int batchAggregated;

        do
        {
            batchAggregated = ProcessBatch();
            totalAggregated += batchAggregated;

            if (batchAggregated >= _batchSize)
            {
                // More records to process, short delay between batches
                cancellationToken.WaitHandle.WaitOne(TimeSpan.FromMilliseconds(500));
                cancellationToken.ThrowIfCancellationRequested();
            }
        }
        while (batchAggregated >= _batchSize && !cancellationToken.IsCancellationRequested);

        if (totalAggregated > 0)
        {
            _logger.InfoFormat("Aggregated {0} counter records.", totalAggregated);
        }

        // Wait for next aggregation cycle
        cancellationToken.WaitHandle.WaitOne(_aggregationInterval);
    }

    internal int ProcessBatch()
    {
        var counterTable = _storage.GetTableName("COUNTER");
        var aggregatedTable = _storage.GetTableName("AGGREGATED_COUNTER");

        try
        {
            using var connection = _storage.CreateAndOpenConnection();
            using var transaction = connection.BeginTransaction(_storage.Options.TransactionIsolationLevel);

            try
            {
                using var command = connection.CreateCommand();
                command.Transaction = transaction;
                command.BindByName = true;
                command.CommandTimeout = _storage.Options.CommandTimeout;
                command.CommandText = $@"DECLARE
  CURSOR counter_rows IS
    SELECT ID, KEY_NAME, VALUE, EXPIRE_AT
    FROM {counterTable}
    ORDER BY ID
    FOR UPDATE SKIP LOCKED;
  current_id {counterTable}.ID%TYPE;
  current_key {counterTable}.KEY_NAME%TYPE;
  current_value {counterTable}.VALUE%TYPE;
  current_expire_at {counterTable}.EXPIRE_AT%TYPE;
  processed NUMBER := 0;
BEGIN
  OPEN counter_rows;
  LOOP
    EXIT WHEN processed >= :batchSize;
    FETCH counter_rows INTO current_id, current_key, current_value, current_expire_at;
    EXIT WHEN counter_rows%NOTFOUND;

    MERGE INTO {aggregatedTable} target
    USING (SELECT current_key KEY_NAME FROM DUAL) source
    ON (target.KEY_NAME = source.KEY_NAME)
    WHEN MATCHED THEN UPDATE SET
      target.VALUE = target.VALUE + current_value,
      target.EXPIRE_AT = CASE
        WHEN target.EXPIRE_AT IS NULL THEN current_expire_at
        WHEN current_expire_at IS NULL THEN target.EXPIRE_AT
        ELSE GREATEST(target.EXPIRE_AT, current_expire_at)
      END
    WHEN NOT MATCHED THEN INSERT (ID, KEY_NAME, VALUE, EXPIRE_AT)
      VALUES ({_storage.GetTableName("AGG_COUNTER_SEQ")}.NEXTVAL, current_key, current_value, current_expire_at);

    DELETE FROM {counterTable} WHERE CURRENT OF counter_rows;
    processed := processed + 1;
  END LOOP;
  CLOSE counter_rows;
  :processed := processed;
END;";
                command.Parameters.Add("batchSize", OracleDbType.Int32, _batchSize, System.Data.ParameterDirection.Input);
                command.Parameters.Add("processed", OracleDbType.Int32, System.Data.ParameterDirection.Output);
                command.ExecuteNonQuery();
                var deleted = Convert.ToInt32(command.Parameters["processed"].Value.ToString());

                transaction.Commit();
                return deleted;
            }
            catch
            {
                transaction.Rollback();
                throw;
            }
        }
        catch (OracleException ex)
        {
            _logger.WarnException("Error during counter aggregation batch.", ex);
            return 0;
        }
    }

    /// <inheritdoc />
    public override string ToString() => nameof(CounterAggregationProcess);
}
