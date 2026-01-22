using Xunit;
using System;

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
    }

    [Fact]
    public void SchemaName_CanBeSet()
    {
        // Arrange
        var options = new OracleStorageOptions();

        // Act
        options.SchemaName = "HANGFIRE";

        // Assert
        Assert.Equal("HANGFIRE", options.SchemaName);
    }

    [Fact]
    public void TablePrefix_CanBeSet()
    {
        // Arrange
        var options = new OracleStorageOptions();

        // Act
        options.TablePrefix = "CUSTOM_";

        // Assert
        Assert.Equal("CUSTOM_", options.TablePrefix);
    }

    [Fact]
    public void InvisibilityTimeout_CanBeSet()
    {
        // Arrange
        var options = new OracleStorageOptions();
        var timeout = TimeSpan.FromMinutes(60);

        // Act
        options.InvisibilityTimeout = timeout;

        // Assert
        Assert.Equal(timeout, options.InvisibilityTimeout);
    }

    [Fact]
    public void QueuePollInterval_CanBeSet()
    {
        // Arrange
        var options = new OracleStorageOptions();
        var interval = TimeSpan.FromSeconds(30);

        // Act
        options.QueuePollInterval = interval;

        // Assert
        Assert.Equal(interval, options.QueuePollInterval);
    }

    [Fact]
    public void PrepareSchemaIfNecessary_CanBeSet()
    {
        // Arrange
        var options = new OracleStorageOptions();

        // Act
        options.PrepareSchemaIfNecessary = false;

        // Assert
        Assert.False(options.PrepareSchemaIfNecessary);
    }

    [Fact]
    public void FetchCount_CanBeSet()
    {
        // Arrange
        var options = new OracleStorageOptions();

        // Act
        options.FetchCount = 5;

        // Assert
        Assert.Equal(5, options.FetchCount);
    }
}
