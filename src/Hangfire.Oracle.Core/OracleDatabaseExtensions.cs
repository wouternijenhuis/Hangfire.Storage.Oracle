using System.Data;
using Dapper;
using Oracle.ManagedDataAccess.Client;

namespace Hangfire.Oracle.Core;

/// <summary>
/// Provides async extension methods for Oracle database operations with built-in retry logic.
/// These methods are optimized for Oracle 19c and higher.
/// </summary>
internal static class OracleDatabaseExtensions
{
    /// <summary>
    /// Executes a SQL command asynchronously with retry logic for transient failures.
    /// </summary>
    /// <param name="storage">The Oracle storage instance.</param>
    /// <param name="sql">The SQL command to execute.</param>
    /// <param name="param">The parameters for the command.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The number of rows affected.</returns>
    public static async Task<int> ExecuteWithRetryAsync(
        this OracleStorage storage,
        string sql,
        object? param = null,
        CancellationToken cancellationToken = default)
    {
        return await ExecuteWithRetryInternalAsync(
            storage,
            async (connection, ct) =>
            {
                var commandDefinition = new CommandDefinition(
                    sql,
                    param,
                    commandTimeout: storage.Options.CommandTimeout,
                    cancellationToken: ct);

                return await connection.ExecuteAsync(commandDefinition).ConfigureAwait(false);
            },
            cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Queries for multiple rows asynchronously with retry logic.
    /// </summary>
    public static async Task<IEnumerable<T>> QueryWithRetryAsync<T>(
        this OracleStorage storage,
        string sql,
        object? param = null,
        CancellationToken cancellationToken = default)
    {
        return await ExecuteWithRetryInternalAsync(
            storage,
            async (connection, ct) =>
            {
                var commandDefinition = new CommandDefinition(
                    sql,
                    param,
                    commandTimeout: storage.Options.CommandTimeout,
                    cancellationToken: ct);

                return await connection.QueryAsync<T>(commandDefinition).ConfigureAwait(false);
            },
            cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Queries for a single row asynchronously with retry logic.
    /// </summary>
    public static async Task<T?> QuerySingleOrDefaultWithRetryAsync<T>(
        this OracleStorage storage,
        string sql,
        object? param = null,
        CancellationToken cancellationToken = default)
    {
        return await ExecuteWithRetryInternalAsync(
            storage,
            async (connection, ct) =>
            {
                var commandDefinition = new CommandDefinition(
                    sql,
                    param,
                    commandTimeout: storage.Options.CommandTimeout,
                    cancellationToken: ct);

                return await connection.QuerySingleOrDefaultAsync<T>(commandDefinition).ConfigureAwait(false);
            },
            cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Executes a scalar query asynchronously with retry logic.
    /// </summary>
    public static async Task<T?> ExecuteScalarWithRetryAsync<T>(
        this OracleStorage storage,
        string sql,
        object? param = null,
        CancellationToken cancellationToken = default)
    {
        return await ExecuteWithRetryInternalAsync(
            storage,
            async (connection, ct) =>
            {
                var commandDefinition = new CommandDefinition(
                    sql,
                    param,
                    commandTimeout: storage.Options.CommandTimeout,
                    cancellationToken: ct);

                return await connection.ExecuteScalarAsync<T>(commandDefinition).ConfigureAwait(false);
            },
            cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Executes an operation within a transaction asynchronously with retry logic.
    /// </summary>
    public static async Task<T> ExecuteInTransactionWithRetryAsync<T>(
        this OracleStorage storage,
        Func<OracleConnection, IDbTransaction, CancellationToken, Task<T>> operation,
        IsolationLevel? isolationLevel = null,
        CancellationToken cancellationToken = default)
    {
        var options = storage.Options;
        var maxAttempts = options.MaxRetryAttempts;
        var attempt = 0;
        Exception? lastException = null;

        while (attempt < maxAttempts)
        {
            cancellationToken.ThrowIfCancellationRequested();

            try
            {
                using var connection = storage.CreateAndOpenConnection();
                using var transaction = connection.BeginTransaction(
                    isolationLevel ?? options.TransactionIsolationLevel);

                try
                {
                    var result = await operation(connection, transaction, cancellationToken)
                        .ConfigureAwait(false);
                    transaction.Commit();
                    return result;
                }
                catch
                {
                    transaction.Rollback();
                    throw;
                }
            }
            catch (OracleException ex) when (OracleErrorCodes.IsTransientError(ex.Number) && attempt < maxAttempts - 1)
            {
                lastException = ex;
                attempt++;

                var delay = OracleErrorCodes.CalculateRetryDelay(
                    attempt,
                    options.RetryDelay,
                    TimeSpan.FromSeconds(30));

                await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
            }
        }

        throw lastException ?? new InvalidOperationException("Operation failed after maximum retry attempts.");
    }

    /// <summary>
    /// Executes an operation within a connection with retry logic for transient failures.
    /// </summary>
    private static async Task<T> ExecuteWithRetryInternalAsync<T>(
        OracleStorage storage,
        Func<OracleConnection, CancellationToken, Task<T>> operation,
        CancellationToken cancellationToken)
    {
        var options = storage.Options;
        var maxAttempts = options.MaxRetryAttempts;
        var attempt = 0;
        Exception? lastException = null;

        while (attempt < maxAttempts)
        {
            cancellationToken.ThrowIfCancellationRequested();

            try
            {
                using var connection = storage.CreateAndOpenConnection();
                return await operation(connection, cancellationToken).ConfigureAwait(false);
            }
            catch (OracleException ex) when (OracleErrorCodes.IsTransientError(ex.Number) && attempt < maxAttempts - 1)
            {
                lastException = ex;
                attempt++;

                var delay = OracleErrorCodes.CalculateRetryDelay(
                    attempt,
                    options.RetryDelay,
                    TimeSpan.FromSeconds(30));

                await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
            }
        }

        throw lastException ?? new InvalidOperationException("Operation failed after maximum retry attempts.");
    }

    /// <summary>
    /// Creates an async enumerable for streaming large result sets.
    /// This is useful for processing large datasets without loading everything into memory.
    /// </summary>
    public static async IAsyncEnumerable<T> QueryStreamAsync<T>(
        this OracleStorage storage,
        string sql,
        object? param = null,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        using var connection = storage.CreateAndOpenConnection();

        // Use Dapper's unbuffered async query for streaming
        // This doesn't load all results into memory at once
        await foreach (var item in connection.QueryUnbufferedAsync<T>(sql, param)
            .WithCancellation(cancellationToken)
            .ConfigureAwait(false))
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return item;
        }
    }
}
