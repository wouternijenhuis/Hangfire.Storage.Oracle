using System;
using System.Data;
using System.IO;
using System.Reflection;
using Oracle.ManagedDataAccess.Client;

namespace Hangfire.Oracle.Core.Schema;

/// <summary>
/// Manages Oracle database schema for Hangfire storage.
/// Handles schema creation, version tracking, and table existence checks.
/// </summary>
public static class OracleSchemaManager
{
    private const string SchemaVersionKey = "schema:version";
    private const int CurrentSchemaVersion = 1;

    /// <summary>
    /// Ensures the Hangfire schema is created in the database.
    /// Will skip creation if tables already exist.
    /// </summary>
    /// <param name="connection">Open database connection.</param>
    /// <param name="tablePrefix">Table name prefix (e.g., "HF_").</param>
    /// <param name="schemaName">Optional schema name for multi-tenant scenarios.</param>
    public static void EnsureSchemaCreated(OracleConnection connection, string tablePrefix = "HF_", string? schemaName = null)
    {
        if (connection == null)
            throw new ArgumentNullException(nameof(connection));
        if (string.IsNullOrEmpty(tablePrefix))
            throw new ArgumentException("Table prefix cannot be empty.", nameof(tablePrefix));

        if (TablesExist(connection, tablePrefix, schemaName))
        {
            // Schema already exists, check for migrations if needed
            return;
        }

        CreateSchema(connection, tablePrefix, schemaName);
    }

    /// <summary>
    /// Checks if Hangfire tables already exist in the database.
    /// </summary>
    /// <param name="connection">Open database connection.</param>
    /// <param name="tablePrefix">Table name prefix.</param>
    /// <param name="schemaName">Optional schema name.</param>
    /// <returns>True if the core tables exist.</returns>
    public static bool TablesExist(OracleConnection connection, string tablePrefix = "HF_", string? schemaName = null)
    {
        if (connection == null)
            throw new ArgumentNullException(nameof(connection));

        var jobTableName = $"{tablePrefix}JOB";
        string checkSql;

        if (!string.IsNullOrEmpty(schemaName))
        {
            checkSql = @"
                SELECT COUNT(*) 
                FROM ALL_TABLES 
                WHERE OWNER = :schemaName AND TABLE_NAME = :tableName";
        }
        else
        {
            checkSql = @"
                SELECT COUNT(*) 
                FROM USER_TABLES 
                WHERE TABLE_NAME = :tableName";
        }

        using var command = new OracleCommand(checkSql, connection);
        
        if (!string.IsNullOrEmpty(schemaName))
        {
            command.Parameters.Add(new OracleParameter("schemaName", schemaName.ToUpperInvariant()));
        }
        command.Parameters.Add(new OracleParameter("tableName", jobTableName.ToUpperInvariant()));

        var count = Convert.ToInt32(command.ExecuteScalar());
        return count > 0;
    }

    /// <summary>
    /// Gets the current schema version from the database.
    /// </summary>
    /// <param name="connection">Open database connection.</param>
    /// <param name="tablePrefix">Table name prefix.</param>
    /// <returns>The schema version, or 0 if not set.</returns>
    public static int GetSchemaVersion(OracleConnection connection, string tablePrefix = "HF_")
    {
        try
        {
            var hashTableName = $"{tablePrefix}HASH";
            var sql = $@"
                SELECT VALUE 
                FROM {hashTableName} 
                WHERE KEY = :key AND FIELD = 'Version'";

            using var command = new OracleCommand(sql, connection);
            command.Parameters.Add(new OracleParameter("key", SchemaVersionKey));

            var result = command.ExecuteScalar();
            return result != null ? Convert.ToInt32(result) : 0;
        }
        catch
        {
            // Table doesn't exist or other error - assume version 0
            return 0;
        }
    }

    private static void CreateSchema(OracleConnection connection, string tablePrefix, string? schemaName)
    {
        var script = LoadInstallScript();

        // Apply customizations
        script = script.Replace("HF_", tablePrefix);

        if (!string.IsNullOrEmpty(schemaName))
        {
            // Prefix table names with schema
            script = AddSchemaPrefix(script, tablePrefix, schemaName);
        }

        // Execute each statement separately
        var statements = SplitStatements(script);

        foreach (var statement in statements)
        {
            var trimmed = statement.Trim();
            if (string.IsNullOrWhiteSpace(trimmed))
                continue;

            // Skip comments
            if (trimmed.StartsWith("--") || trimmed.StartsWith("/*"))
                continue;

            try
            {
                using var command = new OracleCommand(trimmed, connection);
                command.ExecuteNonQuery();
            }
            catch (OracleException ex) when (ex.Number == 955) // ORA-00955: name already used
            {
                // Object already exists, continue
            }
            catch (OracleException ex) when (ex.Number == 942) // ORA-00942: table/view does not exist
            {
                // Referenced object doesn't exist yet, may happen with constraints
            }
        }
    }

    private static string LoadInstallScript()
    {
        var assembly = typeof(OracleSchemaManager).Assembly;
        var resourceName = "Hangfire.Oracle.Core.Scripts.Install.sql";

        using var stream = assembly.GetManifestResourceStream(resourceName);
        if (stream == null)
        {
            throw new InvalidOperationException(
                $"Could not find embedded resource '{resourceName}'. " +
                "Ensure the Install.sql file is marked as an embedded resource.");
        }

        using var reader = new StreamReader(stream);
        return reader.ReadToEnd();
    }

    private static string[] SplitStatements(string script)
    {
        // Split on semicolons but handle PL/SQL blocks
        return script.Split(new[] { ';' }, StringSplitOptions.RemoveEmptyEntries);
    }

    private static string AddSchemaPrefix(string script, string tablePrefix, string schemaName)
    {
        // This is a simple implementation - might need enhancement for complex scripts
        var tableNames = new[]
        {
            "JOB", "JOB_PARAMETER", "JOB_QUEUE", "JOB_STATE",
            "SERVER", "HASH", "LIST", "SET",
            "COUNTER", "AGGREGATED_COUNTER", "DISTRIBUTED_LOCK"
        };

        foreach (var tableName in tableNames)
        {
            var fullTableName = $"{tablePrefix}{tableName}";
            script = script.Replace($" {fullTableName}", $" {schemaName}.{fullTableName}");
            script = script.Replace($"({fullTableName}", $"({schemaName}.{fullTableName}");
        }

        return script;
    }

    /// <summary>
    /// Drops all Hangfire tables from the database.
    /// Use with caution - this will delete all job data.
    /// </summary>
    /// <param name="connection">Open database connection.</param>
    /// <param name="tablePrefix">Table name prefix.</param>
    public static void DropSchema(OracleConnection connection, string tablePrefix = "HF_")
    {
        if (connection == null)
            throw new ArgumentNullException(nameof(connection));

        // Drop in reverse dependency order
        var tablesToDrop = new[]
        {
            "DISTRIBUTED_LOCK",
            "AGGREGATED_COUNTER",
            "COUNTER",
            "SET",
            "LIST",
            "HASH",
            "JOB_STATE",
            "JOB_QUEUE",
            "JOB_PARAMETER",
            "SERVER",
            "JOB"
        };

        foreach (var table in tablesToDrop)
        {
            var fullName = $"{tablePrefix}{table}";
            try
            {
                using var command = new OracleCommand($"DROP TABLE {fullName} CASCADE CONSTRAINTS", connection);
                command.ExecuteNonQuery();
            }
            catch (OracleException ex) when (ex.Number == 942) // Table doesn't exist
            {
                // Ignore - table was already dropped or never existed
            }
        }

        // Drop sequences
        var sequencesToDrop = new[]
        {
            "JOB_SEQ", "JOB_PARAMETER_SEQ", "JOB_QUEUE_SEQ", "JOB_STATE_SEQ",
            "HASH_SEQ", "LIST_SEQ", "SET_SEQ", "COUNTER_SEQ", "AGGREGATED_COUNTER_SEQ"
        };

        foreach (var seq in sequencesToDrop)
        {
            var fullName = $"{tablePrefix}{seq}";
            try
            {
                using var command = new OracleCommand($"DROP SEQUENCE {fullName}", connection);
                command.ExecuteNonQuery();
            }
            catch (OracleException ex) when (ex.Number == 2289) // Sequence doesn't exist
            {
                // Ignore
            }
        }
    }
}
