namespace Hangfire.Oracle.Core.Tests;

public class OracleIdentifierTests
{
    [Theory]
    [InlineData("HF_", "HF_")]
    [InlineData("hangfire$", "HANGFIRE$")]
    [InlineData("A#1", "A#1")]
    public void ValidateNormalizesSafeUnquotedIdentifiers(string value, string expected)
    {
        Assert.Equal(expected, OracleIdentifier.Validate(value, "value"));
    }

    [Theory]
    [InlineData("")]
    [InlineData("1HF")]
    [InlineData("HF-TEST")]
    [InlineData("HF.TEST")]
    [InlineData("HF TEST")]
    public void ValidateRejectsUnsafeIdentifiers(string value)
    {
        var exception = Assert.Throws<ArgumentException>(() => OracleIdentifier.Validate(value, "value"));
        Assert.Equal("value", exception.ParamName);
    }

    [Fact]
    public void ValidateRejectsIdentifiersLongerThanOracleLimit()
    {
        Assert.Throws<ArgumentException>(() => OracleIdentifier.Validate("A" + new string('B', 128), "value"));
    }

    [Fact]
    public void ValidatePrefixAccountsForGeneratedIndexNames()
    {
        var prefixThatOnlyFitsByItself = "H" + new string('F', 103);

        Assert.Throws<ArgumentException>(
            () => OracleIdentifier.ValidatePrefix(prefixThatOnlyFitsByItself, "prefix"));
    }
}
