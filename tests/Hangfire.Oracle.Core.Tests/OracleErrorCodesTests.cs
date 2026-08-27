namespace Hangfire.Oracle.Core.Tests;

public class OracleErrorCodesTests
{
    [Theory]
    [InlineData(OracleErrorCodes.ResourceBusy)]
    [InlineData(OracleErrorCodes.DeadlockDetected)]
    [InlineData(OracleErrorCodes.ConnectTimeout)]
    [InlineData(OracleErrorCodes.ConnectionLostContact)]
    public void TransientErrorsAreClassifiedForRetry(int errorCode)
    {
        Assert.True(OracleErrorCodes.IsTransientError(errorCode));
    }

    [Fact]
    public void PermanentErrorsAreNotClassifiedForRetry()
    {
        Assert.False(OracleErrorCodes.IsTransientError(942));
        Assert.False(OracleErrorCodes.IsTransientError(OracleErrorCodes.SpaceQuotaExceeded));
    }

    [Theory]
    [InlineData(OracleErrorCodes.UniqueConstraintViolated)]
    [InlineData(OracleErrorCodes.ForeignKeyParentNotFound)]
    [InlineData(OracleErrorCodes.ForeignKeyChildRecordFound)]
    public void ConstraintErrorsAreRecognized(int errorCode)
    {
        Assert.True(OracleErrorCodes.IsConstraintViolation(errorCode));
    }

    [Fact]
    public void RetryDelayUsesExponentialBackoffAndCap()
    {
        var delay = OracleErrorCodes.CalculateRetryDelay(10, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(2));

        Assert.InRange(delay, TimeSpan.FromMilliseconds(1600), TimeSpan.FromMilliseconds(2400));
    }
}
