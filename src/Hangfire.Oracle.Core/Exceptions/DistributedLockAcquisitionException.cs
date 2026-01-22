namespace Hangfire.Oracle.Core.Exceptions;

/// <summary>
/// Exception thrown when a distributed lock cannot be acquired within the specified timeout.
/// </summary>
public sealed class DistributedLockAcquisitionException : Exception
{
    /// <summary>
    /// Gets the name of the resource that was being locked.
    /// </summary>
    public string ResourceName { get; }

    /// <summary>
    /// Gets the timeout that was specified for lock acquisition.
    /// </summary>
    public TimeSpan Timeout { get; }

    /// <summary>
    /// Initializes a new instance of the <see cref="DistributedLockAcquisitionException"/> class.
    /// </summary>
    /// <param name="resourceName">The name of the resource that could not be locked.</param>
    /// <param name="timeout">The timeout that was specified for acquiring the lock.</param>
    public DistributedLockAcquisitionException(string resourceName, TimeSpan timeout)
        : base($"Failed to acquire distributed lock on resource '{resourceName}' within {timeout.TotalSeconds:F1} seconds.")
    {
        ResourceName = resourceName;
        Timeout = timeout;
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="DistributedLockAcquisitionException"/> class with an inner exception.
    /// </summary>
    /// <param name="resourceName">The name of the resource that could not be locked.</param>
    /// <param name="timeout">The timeout that was specified for acquiring the lock.</param>
    /// <param name="innerException">The exception that caused this exception.</param>
    public DistributedLockAcquisitionException(string resourceName, TimeSpan timeout, Exception innerException)
        : base($"Failed to acquire distributed lock on resource '{resourceName}' within {timeout.TotalSeconds:F1} seconds.", innerException)
    {
        ResourceName = resourceName;
        Timeout = timeout;
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="DistributedLockAcquisitionException"/> class with a custom message.
    /// </summary>
    /// <param name="message">The error message.</param>
    public DistributedLockAcquisitionException(string message)
        : base(message)
    {
        ResourceName = string.Empty;
        Timeout = TimeSpan.Zero;
    }
}
