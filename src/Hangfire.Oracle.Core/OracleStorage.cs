using System;
using System.Data;
using Hangfire.Storage;
using Oracle.ManagedDataAccess.Client;

namespace Hangfire.Oracle.Core;

/// <summary>
/// Oracle storage implementation for Hangfire
/// </summary>
public class OracleStorage : JobStorage
{
    private readonly string _connectionString;
    private readonly OracleStorageOptions _options;

    /// <summary>
    /// Initializes a new instance of the <see cref="OracleStorage"/> class with the specified connection string.
    /// </summary>
    /// <param name="connectionString">Oracle database connection string</param>
    public OracleStorage(string connectionString)
        : this(connectionString, new OracleStorageOptions())
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="OracleStorage"/> class with the specified connection string and options.
    /// </summary>
    /// <param name="connectionString">Oracle database connection string</param>
    /// <param name="options">Storage options</param>
    public OracleStorage(string connectionString, OracleStorageOptions options)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
            throw new ArgumentNullException(nameof(connectionString));

        _connectionString = connectionString;
        _options = options ?? throw new ArgumentNullException(nameof(options));

        if (_options.PrepareSchemaIfNecessary)
        {
            InitializeSchema();
        }
    }

    /// <summary>
    /// Gets the storage options
    /// </summary>
    public OracleStorageOptions Options => _options;

    /// <summary>
    /// Gets the connection string
    /// </summary>
    public string ConnectionString => _connectionString;

    /// <summary>
    /// Creates a new storage connection
    /// </summary>
    public override IStorageConnection GetConnection()
    {
        return new OracleStorageConnection(this);
    }

    /// <summary>
    /// Creates a new monitoring API instance
    /// </summary>
    public override IMonitoringApi GetMonitoringApi()
    {
        return new OracleMonitoringApi(this);
    }

    /// <summary>
    /// Creates a new Oracle database connection
    /// </summary>
    internal OracleConnection CreateAndOpenConnection()
    {
        var connection = new OracleConnection(_connectionString);
        connection.Open();
        return connection;
    }

    /// <summary>
    /// Gets the table name with optional schema and prefix
    /// </summary>
    internal string GetTableName(string tableName)
    {
        var prefix = _options.TablePrefix ?? "HF_";
        var fullTableName = $"{prefix}{tableName}";

        if (!string.IsNullOrWhiteSpace(_options.SchemaName))
        {
            return $"{_options.SchemaName}.{fullTableName}";
        }

        return fullTableName;
    }

    private void InitializeSchema()
    {
        using var connection = CreateAndOpenConnection();
        
        // Check if tables already exist
        var checkTableSql = $@"
            SELECT COUNT(*) 
            FROM USER_TABLES 
            WHERE TABLE_NAME = '{_options.TablePrefix}JOB'";

        using var checkCommand = new OracleCommand(checkTableSql, connection);
        var tableCount = Convert.ToInt32(checkCommand.ExecuteScalar());

        if (tableCount > 0)
        {
            // Schema already exists
            return;
        }

        // Create schema - read from embedded resource or execute creation scripts
        var scriptContent = GetInstallScript();
        
        // Replace table prefix in script
        scriptContent = scriptContent.Replace("HF_", _options.TablePrefix);

        // Execute script in batches
        var statements = scriptContent.Split(new[] { ';' }, StringSplitOptions.RemoveEmptyEntries);
        foreach (var statement in statements)
        {
            var trimmedStatement = statement.Trim();
            if (string.IsNullOrWhiteSpace(trimmedStatement))
                continue;

            using var command = new OracleCommand(trimmedStatement, connection);
            command.ExecuteNonQuery();
        }
    }

    private string GetInstallScript()
    {
        var assembly = typeof(OracleStorage).Assembly;
        var resourceName = "Hangfire.Oracle.Core.Scripts.Install.sql";
        
        using var stream = assembly.GetManifestResourceStream(resourceName);
        if (stream == null)
        {
            throw new InvalidOperationException($"Could not find embedded resource: {resourceName}");
        }

        using var reader = new System.IO.StreamReader(stream);
        return reader.ReadToEnd();
    }
}
