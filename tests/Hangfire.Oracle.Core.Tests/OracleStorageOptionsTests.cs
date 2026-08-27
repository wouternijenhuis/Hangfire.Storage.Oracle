namespace Hangfire.Oracle.Core.Tests;

public class OracleStorageOptionsTests
{
    [Fact]
    public void Constructor_SetsDefaultValues()
    {
        // Arrange & Act
        var options = new OracleStorageOptions();

        // Assert
        Assert.Equal("HF_", options.TablePrefix);
        Assert.Equal(TimeSpan.FromMinutes(30), options.InvisibilityTimeout);
        Assert.Equal(TimeSpan.FromSeconds(15), options.QueuePollInterval);
        Assert.Equal(TimeSpan.FromMinutes(10), options.DistributedLockTimeout);
        Assert.Equal(TimeSpan.FromMinutes(30), options.JobExpirationCheckInterval);
        Assert.Equal(TimeSpan.FromMinutes(5), options.CounterAggregationInterval);
        Assert.True(options.PrepareSchemaIfNecessary);
        Assert.Equal(TimeSpan.FromMinutes(5), options.SlidingInvisibilityTimeout);
        Assert.Equal(1, options.FetchCount);
        Assert.True(options.UseUtcTime);
        Assert.Null(options.SchemaName);
        Assert.Equal(OracleDatabaseVersion.Oracle19c, options.MinimumDatabaseVersion);
        Assert.True(options.SupportsSkipLocked);
        Assert.False(options.SupportsPartialIndexes);
    }
}
