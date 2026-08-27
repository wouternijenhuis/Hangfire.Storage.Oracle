namespace Hangfire.Oracle.Core.Tests;

public class OracleStorageOptionsValidationTests
{
    [Fact]
    public void ValidateNormalizesSchemaAndPrefix()
    {
        var options = new OracleStorageOptions
        {
            SchemaName = "hangfire",
            TablePrefix = "jobs_"
        };

        options.Validate();

        Assert.Equal("HANGFIRE", options.SchemaName);
        Assert.Equal("JOBS_", options.TablePrefix);
    }

    [Theory]
    [InlineData(OracleDatabaseVersion.Oracle11g)]
    [InlineData(OracleDatabaseVersion.Oracle12c)]
    [InlineData(OracleDatabaseVersion.Oracle18c)]
    public void ValidateRejectsUnsupportedOracleVersions(OracleDatabaseVersion version)
    {
        var options = new OracleStorageOptions { MinimumDatabaseVersion = version };
        Assert.Throws<ArgumentOutOfRangeException>(options.Validate);
    }

    [Fact]
    public void ValidateRejectsUnsafeSchemaAndPrefix()
    {
        Assert.Throws<ArgumentException>(() => new OracleStorageOptions { SchemaName = "HF;DROP" }.Validate());
        Assert.Throws<ArgumentException>(() => new OracleStorageOptions { TablePrefix = "HF-" }.Validate());
    }

    [Fact]
    public void ValidateRejectsNonPositiveOperationalValues()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new OracleStorageOptions { FetchCount = 0 }.Validate());
        Assert.Throws<ArgumentOutOfRangeException>(() => new OracleStorageOptions { CleanupBatchSize = 0 }.Validate());
        Assert.Throws<ArgumentOutOfRangeException>(() => new OracleStorageOptions { InvisibilityTimeout = TimeSpan.Zero }.Validate());
        Assert.Throws<ArgumentOutOfRangeException>(() => new OracleStorageOptions { CommandTimeout = -1 }.Validate());
    }
}
