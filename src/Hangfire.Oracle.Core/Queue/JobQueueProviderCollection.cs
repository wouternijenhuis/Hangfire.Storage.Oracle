using System.Collections;

namespace Hangfire.Oracle.Core.Queue;

/// <summary>
/// Collection of job queue providers with default provider support.
/// Allows registration of multiple providers for different queue types.
/// </summary>
public sealed class JobQueueProviderCollection : IEnumerable<IJobQueueProvider>
{
    private readonly Dictionary<string, IJobQueueProvider> _providers = new(StringComparer.OrdinalIgnoreCase);
    private readonly IJobQueueProvider _defaultProvider;

    /// <summary>
    /// Creates a new collection with the specified default provider.
    /// </summary>
    /// <param name="defaultProvider">The default provider to use for unregistered queues.</param>
    public JobQueueProviderCollection(IJobQueueProvider defaultProvider)
    {
        _defaultProvider = defaultProvider ?? throw new ArgumentNullException(nameof(defaultProvider));
    }

    /// <summary>
    /// Registers a provider for specific queue names.
    /// </summary>
    /// <param name="provider">The queue provider.</param>
    /// <param name="queues">Queue names that should use this provider.</param>
    public void Register(IJobQueueProvider provider, params string[] queues)
    {
        if (provider == null)
        {
            throw new ArgumentNullException(nameof(provider));
        }

        if (queues == null || queues.Length == 0)
        {
            throw new ArgumentException("At least one queue name must be specified.", nameof(queues));
        }

        foreach (var queue in queues)
        {
            if (string.IsNullOrWhiteSpace(queue))
            {
                throw new ArgumentException("Queue name cannot be empty.", nameof(queues));
            }

            _providers[queue] = provider;
        }
    }

    /// <summary>
    /// Gets the provider for the specified queue.
    /// Returns the registered provider or the default provider if none is registered.
    /// </summary>
    /// <param name="queue">The queue name.</param>
    /// <returns>The appropriate job queue provider.</returns>
    public IJobQueueProvider GetProvider(string queue)
    {
        if (string.IsNullOrEmpty(queue))
        {
            return _defaultProvider;
        }

        return _providers.TryGetValue(queue, out var provider) ? provider : _defaultProvider;
    }

    /// <summary>
    /// Gets the default queue provider.
    /// </summary>
    public IJobQueueProvider DefaultProvider => _defaultProvider;

    /// <inheritdoc/>
    public IEnumerator<IJobQueueProvider> GetEnumerator()
    {
        var seen = new HashSet<IJobQueueProvider> { _defaultProvider };
        yield return _defaultProvider;

        foreach (var provider in _providers.Values)
        {
            if (seen.Add(provider))
            {
                yield return provider;
            }
        }
    }

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}
