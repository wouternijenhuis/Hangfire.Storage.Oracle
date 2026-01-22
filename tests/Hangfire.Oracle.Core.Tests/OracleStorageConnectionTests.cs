namespace Hangfire.Oracle.Core.Tests;

public class OracleStorageConnectionTests
{
    [Fact]
    public void Constructor_ThrowsException_WhenStorageIsNull()
    {
        // Arrange, Act & Assert
        var exception = Assert.Throws<ArgumentNullException>(() => new OracleStorageConnection(null!));
        Assert.Equal("storage", exception.ParamName);
    }

    [Fact]
    public void CreateWriteTransaction_ReturnsOracleWriteOnlyTransaction()
    {
        // Arrange
        var options = new OracleStorageOptions
        {
            PrepareSchemaIfNecessary = false
        };
        var storage = new OracleStorage("Data Source=test", options);
        var connection = new OracleStorageConnection(storage);

        // Act
        var transaction = connection.CreateWriteTransaction();

        // Assert
        Assert.NotNull(transaction);
        Assert.IsType<OracleWriteOnlyTransaction>(transaction);
    }
}
