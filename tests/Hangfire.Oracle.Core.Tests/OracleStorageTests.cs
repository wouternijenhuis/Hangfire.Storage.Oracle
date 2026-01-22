namespace Hangfire.Oracle.Core.Tests;

public class OracleStorageTests
{
    [Fact]
    public void Constructor_ThrowsException_WhenConnectionStringIsNull()
    {
        // Arrange, Act & Assert
        var exception = Assert.Throws<ArgumentNullException>(() => new OracleStorage(null!));
        Assert.Equal("connectionString", exception.ParamName);
    }

    [Fact]
    public void Constructor_ThrowsException_WhenConnectionStringIsEmpty()
    {
        // Arrange, Act & Assert
        var exception = Assert.Throws<ArgumentNullException>(() => new OracleStorage(string.Empty));
        Assert.Equal("connectionString", exception.ParamName);
    }

    [Fact]
    public void Constructor_ThrowsException_WhenConnectionStringIsWhitespace()
    {
        // Arrange, Act & Assert
        var exception = Assert.Throws<ArgumentNullException>(() => new OracleStorage("   "));
        Assert.Equal("connectionString", exception.ParamName);
    }

    [Fact]
    public void Constructor_ThrowsException_WhenOptionsIsNull()
    {
        // Arrange, Act & Assert
        var exception = Assert.Throws<ArgumentNullException>(
            () => new OracleStorage("Data Source=test", null!));
        Assert.Equal("options", exception.ParamName);
    }

    [Fact]
    public void Options_ReturnsConfiguredOptions()
    {
        // Arrange
        var options = new OracleStorageOptions
        {
            SchemaName = "TEST_SCHEMA",
            TablePrefix = "TEST_",
            PrepareSchemaIfNecessary = false
        };
        var storage = new OracleStorage("Data Source=test", options);

        // Act
        var result = storage.Options;

        // Assert
        Assert.Same(options, result);
        Assert.Equal("TEST_SCHEMA", result.SchemaName);
        Assert.Equal("TEST_", result.TablePrefix);
    }

    [Fact]
    public void ConnectionString_ReturnsConfiguredConnectionString()
    {
        // Arrange
        var connectionString = "Data Source=test;User Id=user;Password=pass";
        var options = new OracleStorageOptions
        {
            PrepareSchemaIfNecessary = false
        };
        var storage = new OracleStorage(connectionString, options);

        // Act
        var result = storage.ConnectionString;

        // Assert - connection string is enhanced with pooling settings
        Assert.Contains("DATA SOURCE=test", result, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("USER ID=user", result, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("PASSWORD=pass", result, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void GetConnection_ReturnsOracleStorageConnection()
    {
        // Arrange
        var options = new OracleStorageOptions
        {
            PrepareSchemaIfNecessary = false
        };
        var storage = new OracleStorage("Data Source=test", options);

        // Act
        var connection = storage.GetConnection();

        // Assert
        Assert.NotNull(connection);
        Assert.IsType<OracleStorageConnection>(connection);
    }

    [Fact]
    public void GetMonitoringApi_ReturnsOracleMonitoringApi()
    {
        // Arrange
        var options = new OracleStorageOptions
        {
            PrepareSchemaIfNecessary = false
        };
        var storage = new OracleStorage("Data Source=test", options);

        // Act
        var monitoringApi = storage.GetMonitoringApi();

        // Assert
        Assert.NotNull(monitoringApi);
        Assert.IsType<OracleMonitoringApi>(monitoringApi);
    }
}
