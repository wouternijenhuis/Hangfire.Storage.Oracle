namespace Hangfire.Oracle.Core;

/// <summary>
/// Extension methods for configuring Hangfire to use Oracle storage
/// </summary>
public static class OracleStorageExtensions
{
    /// <summary>
    /// Configures Hangfire to use Oracle storage with default options
    /// </summary>
    /// <param name="configuration">Global configuration</param>
    /// <param name="connectionString">Oracle database connection string</param>
    /// <returns>The configuration instance for method chaining</returns>
    public static IGlobalConfiguration<OracleStorage> UseOracleStorage(
        this IGlobalConfiguration configuration,
        string connectionString)
    {
        if (configuration == null)
        {
            throw new ArgumentNullException(nameof(configuration));
        }

        if (connectionString == null)
        {
            throw new ArgumentNullException(nameof(connectionString));
        }

        var storage = new OracleStorage(connectionString);
        return configuration.UseStorage(storage);
    }

    /// <summary>
    /// Configures Hangfire to use Oracle storage with custom options
    /// </summary>
    /// <param name="configuration">Global configuration</param>
    /// <param name="connectionString">Oracle database connection string</param>
    /// <param name="options">Storage configuration options</param>
    /// <returns>The configuration instance for method chaining</returns>
    public static IGlobalConfiguration<OracleStorage> UseOracleStorage(
        this IGlobalConfiguration configuration,
        string connectionString,
        OracleStorageOptions options)
    {
        if (configuration == null)
        {
            throw new ArgumentNullException(nameof(configuration));
        }

        if (connectionString == null)
        {
            throw new ArgumentNullException(nameof(connectionString));
        }

        if (options == null)
        {
            throw new ArgumentNullException(nameof(options));
        }

        var storage = new OracleStorage(connectionString, options);
        return configuration.UseStorage(storage);
    }
}
