using Hangfire.Oracle.Core.Schema;

namespace Hangfire.Oracle.Core.Tests;

public class OracleSchemaManagerTests
{
    [Fact]
    public void SplitStatementsPreservesDdlAfterComments()
    {
        const string Script = "-- job table\nCREATE TABLE HF_JOB (ID NUMBER);\n-- sequence\nCREATE SEQUENCE HF_JOB_SEQ;";

        var statements = OracleSchemaManager.SplitStatements(Script);

        Assert.Equal(2, statements.Count);
        Assert.StartsWith("CREATE TABLE", statements[0], StringComparison.Ordinal);
        Assert.StartsWith("CREATE SEQUENCE", statements[1], StringComparison.Ordinal);
    }

    [Fact]
    public void SplitStatementsDoesNotSplitQuotedSemicolons()
    {
        const string Script = "INSERT INTO T VALUES ('a;b'); /* ; */ INSERT INTO T VALUES ('it''s fine');";

        var statements = OracleSchemaManager.SplitStatements(Script);

        Assert.Equal(2, statements.Count);
        Assert.Contains("'a;b'", statements[0], StringComparison.Ordinal);
        Assert.Contains("'it''s fine'", statements[1], StringComparison.Ordinal);
    }

    [Fact]
    public void SplitStatementsRejectsUnterminatedContent()
    {
        Assert.Throws<InvalidOperationException>(() => OracleSchemaManager.SplitStatements("SELECT 'unterminated"));
        Assert.Throws<InvalidOperationException>(() => OracleSchemaManager.SplitStatements("/* unterminated"));
    }

    [Fact]
    public void SplitStatementsIgnoresEmptyAndCommentOnlySegments()
    {
        Assert.Empty(OracleSchemaManager.SplitStatements("; -- only a comment\n ; /* block */ ;"));
    }
}
