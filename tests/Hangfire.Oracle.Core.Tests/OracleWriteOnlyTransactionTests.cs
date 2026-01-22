namespace Hangfire.Oracle.Core.Tests;

public class OracleWriteOnlyTransactionTests
{
    [Fact]
    public void Constructor_ThrowsException_WhenStorageIsNull()
    {
        // Arrange, Act & Assert
        var exception = Assert.Throws<ArgumentNullException>(() => new OracleWriteOnlyTransaction(null!));
        Assert.Equal("storage", exception.ParamName);
    }
}
