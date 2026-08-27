using System.Text;
using Dapper;
using Oracle.ManagedDataAccess.Client;

namespace Hangfire.Oracle.Core.Schema;

/// <summary>
/// Installs and migrates the Oracle database objects used by Hangfire.
/// </summary>
public static class OracleSchemaManager
{
    private const string SchemaVersionKey = "schema:version";
    private const int CurrentSchemaVersion = 2;

    private static readonly string[] _requiredTables =
    {
        "JOB", "JOB_STATE", "JOB_PARAMETER", "JOB_QUEUE", "SERVER", "SET",
        "COUNTER", "HASH", "LIST", "AGGREGATED_COUNTER", "DISTRIBUTED_LOCK"
    };

    private static readonly string[] _requiredSequences =
    {
        "JOB_SEQ", "JOB_STATE_SEQ", "JOB_PARAMETER_SEQ", "JOB_QUEUE_SEQ", "SET_SEQ",
        "COUNTER_SEQ", "HASH_SEQ", "LIST_SEQ", "AGG_COUNTER_SEQ"
    };

    /// <summary>
    /// Creates missing objects and applies data-preserving migrations.
    /// </summary>
    public static void EnsureSchemaCreated(
        OracleConnection connection,
        string tablePrefix = "HF_",
        string? schemaName = null)
    {
        EnsureSchemaCreated(connection, tablePrefix, schemaName, 30);
    }

    /// <summary>
    /// Creates missing objects and applies data-preserving migrations using the specified command timeout.
    /// </summary>
    public static void EnsureSchemaCreated(
        OracleConnection connection,
        string tablePrefix,
        string? schemaName,
        int commandTimeout)
    {
        ArgumentNullException.ThrowIfNull(connection);
        ArgumentOutOfRangeException.ThrowIfNegative(commandTimeout);

        var prefix = OracleIdentifier.ValidatePrefix(tablePrefix, nameof(tablePrefix));
        var schema = string.IsNullOrWhiteSpace(schemaName)
            ? null
            : OracleIdentifier.Validate(schemaName, nameof(schemaName));

        EnsureOpen(connection);
        SetCurrentSchema(connection, schema, commandTimeout);

        if (ObjectExists(connection, "TABLE", prefix + "DISTRIBUTED_LOCK", schema, commandTimeout))
        {
            MigrateFromVersionOne(connection, prefix, commandTimeout);
        }

        foreach (var statement in SplitStatements(LoadInstallScript().Replace("HF_", prefix, StringComparison.Ordinal)))
        {
            ExecuteDdl(connection, statement, commandTimeout, 955);
        }

        MigrateFromVersionOne(connection, prefix, commandTimeout);
        SetSchemaVersion(connection, prefix, commandTimeout);
        VerifySchema(connection, prefix, schema, commandTimeout);
    }

    /// <summary>
    /// Returns whether every required table and sequence exists.
    /// </summary>
    public static bool TablesExist(
        OracleConnection connection,
        string tablePrefix = "HF_",
        string? schemaName = null)
    {
        ArgumentNullException.ThrowIfNull(connection);
        var prefix = OracleIdentifier.ValidatePrefix(tablePrefix, nameof(tablePrefix));
        var schema = string.IsNullOrWhiteSpace(schemaName)
            ? null
            : OracleIdentifier.Validate(schemaName, nameof(schemaName));

        EnsureOpen(connection);
        return _requiredTables.All(name => ObjectExists(connection, "TABLE", prefix + name, schema, 30))
            && _requiredSequences.All(name => ObjectExists(connection, "SEQUENCE", prefix + name, schema, 30));
    }

    /// <summary>
    /// Gets the installed schema version, or zero when the schema/version row is absent.
    /// </summary>
    public static int GetSchemaVersion(OracleConnection connection, string tablePrefix = "HF_")
    {
        ArgumentNullException.ThrowIfNull(connection);
        var prefix = OracleIdentifier.ValidatePrefix(tablePrefix, nameof(tablePrefix));
        EnsureOpen(connection);

        try
        {
            var value = connection.ExecuteScalar<string?>(
                $"SELECT VALUE FROM {prefix}HASH WHERE KEY_NAME = :key AND FIELD = 'Version'",
                new { key = SchemaVersionKey },
                commandTimeout: 30);
            return int.TryParse(value, out var version) ? version : 0;
        }
        catch (OracleException ex) when (ex.Number == 942)
        {
            return 0;
        }
    }

    /// <summary>
    /// Drops all Hangfire tables and sequences in the current schema.
    /// </summary>
    public static void DropSchema(OracleConnection connection, string tablePrefix = "HF_")
    {
        ArgumentNullException.ThrowIfNull(connection);
        var prefix = OracleIdentifier.ValidatePrefix(tablePrefix, nameof(tablePrefix));
        EnsureOpen(connection);

        foreach (var table in _requiredTables.AsEnumerable().Reverse())
        {
            ExecuteDdl(connection, $"DROP TABLE {prefix}{table} CASCADE CONSTRAINTS PURGE", 30, 942);
        }

        foreach (var sequence in _requiredSequences)
        {
            ExecuteDdl(connection, $"DROP SEQUENCE {prefix}{sequence}", 30, 2289);
        }
    }

    internal static IReadOnlyList<string> SplitStatements(string script)
    {
        ArgumentNullException.ThrowIfNull(script);

        var statements = new List<string>();
        var current = new StringBuilder();
        var inString = false;
        var inLineComment = false;
        var inBlockComment = false;

        for (var index = 0; index < script.Length; index++)
        {
            var character = script[index];
            var next = index + 1 < script.Length ? script[index + 1] : '\0';

            if (inLineComment)
            {
                if (character is '\r' or '\n')
                {
                    inLineComment = false;
                    current.Append(' ');
                }

                continue;
            }

            if (inBlockComment)
            {
                if (character == '*' && next == '/')
                {
                    inBlockComment = false;
                    index++;
                    current.Append(' ');
                }

                continue;
            }

            if (!inString && character == '-' && next == '-')
            {
                inLineComment = true;
                index++;
                continue;
            }

            if (!inString && character == '/' && next == '*')
            {
                inBlockComment = true;
                index++;
                continue;
            }

            if (character == '\'')
            {
                current.Append(character);
                if (inString && next == '\'')
                {
                    current.Append(next);
                    index++;
                    continue;
                }

                inString = !inString;
                continue;
            }

            if (!inString && character == ';')
            {
                AddStatement(statements, current);
                continue;
            }

            current.Append(character);
        }

        if (inString || inBlockComment)
        {
            throw new InvalidOperationException("The schema script contains an unterminated string or block comment.");
        }

        AddStatement(statements, current);
        return statements;
    }

    private static void AddStatement(ICollection<string> statements, StringBuilder current)
    {
        var statement = current.ToString().Trim();
        current.Clear();
        if (statement.Length > 0)
        {
            statements.Add(statement);
        }
    }

    private static void MigrateFromVersionOne(OracleConnection connection, string prefix, int commandTimeout)
    {
        var lockTable = prefix + "DISTRIBUTED_LOCK";
        if (!ColumnExists(connection, lockTable, "OWNER_ID", commandTimeout))
        {
            ExecuteDdl(connection, $"ALTER TABLE {lockTable} ADD OWNER_ID NVARCHAR2(36)", commandTimeout, 1430);
            connection.Execute(
                $"UPDATE {lockTable} SET OWNER_ID = RAWTOHEX(SYS_GUID()) WHERE OWNER_ID IS NULL",
                commandTimeout: commandTimeout);
            ExecuteDdl(connection, $"ALTER TABLE {lockTable} MODIFY OWNER_ID NOT NULL", commandTimeout);
        }

        if (!ColumnExists(connection, lockTable, "EXPIRE_AT", commandTimeout))
        {
            ExecuteDdl(connection, $"ALTER TABLE {lockTable} ADD EXPIRE_AT TIMESTAMP(7)", commandTimeout, 1430);
            connection.Execute(
                $"UPDATE {lockTable} SET EXPIRE_AT = CREATED_AT WHERE EXPIRE_AT IS NULL",
                commandTimeout: commandTimeout);
            ExecuteDdl(connection, $"ALTER TABLE {lockTable} MODIFY EXPIRE_AT NOT NULL", commandTimeout);
        }
    }

    private static void SetSchemaVersion(OracleConnection connection, string prefix, int commandTimeout)
    {
        connection.Execute(
            $@"MERGE INTO {prefix}HASH h
               USING (SELECT :key KEY_NAME, 'Version' FIELD FROM DUAL) source
               ON (h.KEY_NAME = source.KEY_NAME AND h.FIELD = source.FIELD)
               WHEN MATCHED THEN UPDATE SET h.VALUE = :value, h.EXPIRE_AT = NULL
               WHEN NOT MATCHED THEN INSERT (ID, KEY_NAME, FIELD, VALUE, EXPIRE_AT)
                 VALUES ({prefix}HASH_SEQ.NEXTVAL, :key, 'Version', :value, NULL)",
            new { key = SchemaVersionKey, value = CurrentSchemaVersion.ToString() },
            commandTimeout: commandTimeout);
    }

    private static void VerifySchema(
        OracleConnection connection,
        string prefix,
        string? schema,
        int commandTimeout)
    {
        var missing = _requiredTables
            .Where(name => !ObjectExists(connection, "TABLE", prefix + name, schema, commandTimeout))
            .Select(name => prefix + name)
            .Concat(_requiredSequences
                .Where(name => !ObjectExists(connection, "SEQUENCE", prefix + name, schema, commandTimeout))
                .Select(name => prefix + name))
            .ToArray();

        if (missing.Length > 0)
        {
            throw new InvalidOperationException($"Oracle schema installation is incomplete. Missing: {string.Join(", ", missing)}.");
        }
    }

    private static bool ObjectExists(
        OracleConnection connection,
        string objectType,
        string objectName,
        string? schema,
        int commandTimeout)
    {
        var owner = schema ?? connection.ExecuteScalar<string>(
            "SELECT SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') FROM DUAL",
            commandTimeout: commandTimeout)
            ?? throw new InvalidOperationException("Oracle did not return a current schema.");
        return connection.ExecuteScalar<int>(
            @"SELECT COUNT(*) FROM ALL_OBJECTS
              WHERE OWNER = :owner AND OBJECT_TYPE = :objectType AND OBJECT_NAME = :objectName",
            new { owner = owner.ToUpperInvariant(), objectType, objectName = objectName.ToUpperInvariant() },
            commandTimeout: commandTimeout) > 0;
    }

    private static bool ColumnExists(
        OracleConnection connection,
        string tableName,
        string columnName,
        int commandTimeout)
    {
        var owner = connection.ExecuteScalar<string>(
            "SELECT SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') FROM DUAL",
            commandTimeout: commandTimeout)
            ?? throw new InvalidOperationException("Oracle did not return a current schema.");
        return connection.ExecuteScalar<int>(
            @"SELECT COUNT(*) FROM ALL_TAB_COLUMNS
              WHERE OWNER = :owner AND TABLE_NAME = :tableName AND COLUMN_NAME = :columnName",
            new { owner = owner.ToUpperInvariant(), tableName = tableName.ToUpperInvariant(), columnName },
            commandTimeout: commandTimeout) > 0;
    }

    private static void SetCurrentSchema(OracleConnection connection, string? schema, int commandTimeout)
    {
        if (schema is null)
        {
            return;
        }

        using var command = connection.CreateCommand();
        command.CommandText = $"ALTER SESSION SET CURRENT_SCHEMA = {schema}";
        command.CommandTimeout = commandTimeout;
        command.ExecuteNonQuery();
    }

    private static void ExecuteDdl(
        OracleConnection connection,
        string statement,
        int commandTimeout,
        params int[] allowedErrorNumbers)
    {
        try
        {
            using var command = connection.CreateCommand();
            command.CommandText = statement;
            command.CommandTimeout = commandTimeout;
            command.ExecuteNonQuery();
        }
        catch (OracleException ex) when (allowedErrorNumbers.Contains(ex.Number))
        {
            // The desired idempotent end-state already exists (or is already absent for drops).
        }
    }

    private static void EnsureOpen(OracleConnection connection)
    {
        if (connection.State != System.Data.ConnectionState.Open)
        {
            throw new InvalidOperationException("The Oracle connection must be open.");
        }
    }

    private static string LoadInstallScript()
    {
        const string ResourceName = "Hangfire.Oracle.Core.Scripts.Install.sql";
        using var stream = typeof(OracleSchemaManager).Assembly.GetManifestResourceStream(ResourceName)
            ?? throw new InvalidOperationException($"Could not find embedded resource '{ResourceName}'.");
        using var reader = new StreamReader(stream);
        return reader.ReadToEnd();
    }
}
