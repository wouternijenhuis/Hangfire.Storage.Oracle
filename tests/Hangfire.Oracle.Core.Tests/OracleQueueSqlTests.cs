using Hangfire.Oracle.Core.Queue;

namespace Hangfire.Oracle.Core.Tests;

public class OracleQueueSqlTests
{
    private static readonly string[] _multipleQueues = { "default", "critical" };
    private static readonly string[] _defaultQueue = { "default" };

    [Fact]
    public void FetchBlockUsesSkipLockedWithoutRowLimitingClause()
    {
        using var storage = CreateStorage(useSkipLocked: true);
        var queue = new OracleQueue(storage, storage.Options);

        var sql = queue.BuildFetchBlock(_multipleQueues);

        Assert.Contains("FOR UPDATE SKIP LOCKED", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("FETCH FIRST", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains(":queue0, :queue1", sql, StringComparison.Ordinal);
        Assert.Contains("WHERE CURRENT OF next_job", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void ClassicFetchBlockUsesNowait()
    {
        using var storage = CreateStorage(useSkipLocked: false);
        var queue = new OracleQueue(storage, storage.Options);

        var sql = queue.BuildFetchBlock(_defaultQueue);

        Assert.Contains("FOR UPDATE NOWAIT", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("SKIP LOCKED", sql, StringComparison.Ordinal);
    }

    private static OracleStorage CreateStorage(bool useSkipLocked)
    {
        return new OracleStorage(
            "Data Source=test;User Id=user;Password=password",
            new OracleStorageOptions
            {
                PrepareSchemaIfNecessary = false,
                UseSkipLocked = useSkipLocked
            });
    }
}
