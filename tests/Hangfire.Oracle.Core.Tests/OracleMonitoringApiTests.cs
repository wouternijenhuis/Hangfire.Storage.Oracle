namespace Hangfire.Oracle.Core.Tests;

public class OracleMonitoringApiTests
{
    [Fact]
    public void Constructor_ThrowsException_WhenStorageIsNull()
    {
        // Arrange, Act & Assert
        var exception = Assert.Throws<ArgumentNullException>(() => new OracleMonitoringApi(null!));
        Assert.Equal("storage", exception.ParamName);
    }
}
