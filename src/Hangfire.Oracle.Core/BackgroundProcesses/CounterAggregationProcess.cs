using System;
using System.Threading;
using Dapper;
using Hangfire.Logging;
using Hangfire.Server;

namespace Hangfire.Oracle.Core.BackgroundProcesses;

/// <summary>
/// Background process that aggregates individual counter records into summarized counters.
/// This reduces the number of rows in the counter table and improves query performance.
/// </summary>
#pragma warning disable CS0618 // IServerComponent is obsolete but still required
internal sealed class CounterAggregationProcess : IServerComponent
#pragma warning restore CS0618
{
    private static readonly ILog Logger = LogProvider.GetLogger(typeof(CounterAggregationProcess));
    
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
        Logger.Debug("Starting counter aggregation...");

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
            Logger.InfoFormat("Aggregated {0} counter records.", totalAggregated);
        }

        // Wait for next aggregation cycle
        cancellationToken.WaitHandle.WaitOne(_aggregationInterval);
    }

    private int ProcessBatch()
    {
        var counterTable = _storage.GetTableName("COUNTER");
        var aggregatedTable = _storage.GetTableName("AGGREGATED_COUNTER");

        try
        {
            using var connection = _storage.CreateAndOpenConnection();
            using var transaction = connection.BeginTransaction();

            try
            {
                // Step 1: Merge counters into aggregated table
                // Group by KEY and sum values, handling expiration
                var mergeSql = $@"
                    MERGE INTO {aggregatedTable} dest
                    USING (
                        SELECT KEY, SUM(VALUE) AS TOTAL_VALUE, MAX(EXPIRE_AT) AS MAX_EXPIRE
                        FROM (
                            SELECT KEY, VALUE, EXPIRE_AT
                            FROM {counterTable}
                            WHERE ROWNUM <= :batchSize
                        )
                        GROUP BY KEY
                    ) src
                    ON (dest.KEY = src.KEY)
                    WHEN MATCHED THEN
                        UPDATE SET 
                            dest.VALUE = dest.VALUE + src.TOTAL_VALUE,
                            dest.EXPIRE_AT = GREATEST(dest.EXPIRE_AT, src.MAX_EXPIRE)
                    WHEN NOT MATCHED THEN
                        INSERT (KEY, VALUE, EXPIRE_AT)
                        VALUES (src.KEY, src.TOTAL_VALUE, src.MAX_EXPIRE)";

                connection.Execute(mergeSql, new { batchSize = _batchSize }, transaction);

                // Step 2: Delete the processed counter records
                // We use a subquery to identify the exact records we just aggregated
                var deleteSql = $@"
                    DELETE FROM {counterTable}
                    WHERE ROWID IN (
                        SELECT ROWID FROM {counterTable}
                        WHERE ROWNUM <= :batchSize
                    )";

                var deleted = connection.Execute(deleteSql, new { batchSize = _batchSize }, transaction);

                transaction.Commit();
                return deleted;
            }
            catch
            {
                transaction.Rollback();
                throw;
            }
        }
        catch (Exception ex)
        {
            Logger.WarnException("Error during counter aggregation batch.", ex);
            return 0;
        }
    }

    /// <inheritdoc />
    public override string ToString() => nameof(CounterAggregationProcess);
}
