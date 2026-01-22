namespace Hangfire.Oracle.Core.Queue;

/// <summary>
/// Oracle-specific implementation of the job queue provider.
/// Creates instances of OracleQueue and OracleQueueMonitor.
/// </summary>
internal sealed class OracleJobQueueProvider : IJobQueueProvider
{
    private readonly OracleStorage _storage;
    private readonly OracleStorageOptions _options;
    private readonly Lazy<OracleQueue> _queue;
    private readonly Lazy<OracleQueueMonitor> _monitor;

    public OracleJobQueueProvider(OracleStorage storage, OracleStorageOptions options)
    {
        _storage = storage ?? throw new ArgumentNullException(nameof(storage));
        _options = options ?? throw new ArgumentNullException(nameof(options));

        _queue = new Lazy<OracleQueue>(() => new OracleQueue(_storage, _options));
        _monitor = new Lazy<OracleQueueMonitor>(() => new OracleQueueMonitor(_storage));
    }

    /// <inheritdoc />
    public IJobQueue GetQueue() => _queue.Value;

    /// <inheritdoc />
    public IQueueMonitor GetMonitor() => _monitor.Value;
}
