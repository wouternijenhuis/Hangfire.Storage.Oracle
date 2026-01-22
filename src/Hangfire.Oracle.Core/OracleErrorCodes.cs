using System;
using Oracle.ManagedDataAccess.Client;

namespace Hangfire.Oracle.Core;

/// <summary>
/// Provides Oracle-specific error code handling and classification.
/// Supports Oracle 19c and higher error codes.
/// </summary>
internal static class OracleErrorCodes
{
    // ============================================================
    // Constraint Violations
    // ============================================================

    /// <summary>ORA-00001: unique constraint violated.</summary>
    public const int UniqueConstraintViolated = 1;

    /// <summary>ORA-02291: integrity constraint violated - parent key not found.</summary>
    public const int ForeignKeyParentNotFound = 2291;

    /// <summary>ORA-02292: integrity constraint violated - child record found.</summary>
    public const int ForeignKeyChildRecordFound = 2292;

    // ============================================================
    // Lock and Concurrency Errors
    // ============================================================

    /// <summary>ORA-00054: resource busy and acquire with NOWAIT specified or timeout expired.</summary>
    public const int ResourceBusy = 54;

    /// <summary>ORA-00060: deadlock detected while waiting for resource.</summary>
    public const int DeadlockDetected = 60;

    /// <summary>ORA-30006: resource busy; acquire with WAIT timeout expired.</summary>
    public const int WaitTimeoutExpired = 30006;

    // ============================================================
    // Connection and Network Errors
    // ============================================================

    /// <summary>ORA-12170: TNS:Connect timeout occurred.</summary>
    public const int ConnectTimeout = 12170;

    /// <summary>ORA-12571: TNS:packet writer failure.</summary>
    public const int PacketWriterFailure = 12571;

    /// <summary>ORA-12543: TNS:destination host unreachable.</summary>
    public const int HostUnreachable = 12543;

    /// <summary>ORA-03113: end-of-file on communication channel.</summary>
    public const int EndOfFileOnChannel = 3113;

    /// <summary>ORA-03114: not connected to ORACLE.</summary>
    public const int NotConnected = 3114;

    /// <summary>ORA-03135: connection lost contact.</summary>
    public const int ConnectionLostContact = 3135;

    /// <summary>ORA-01012: not logged on.</summary>
    public const int NotLoggedOn = 1012;

    /// <summary>ORA-01033: ORACLE initialization or shutdown in progress.</summary>
    public const int DatabaseStartingUp = 1033;

    /// <summary>ORA-01034: ORACLE not available.</summary>
    public const int DatabaseNotAvailable = 1034;

    /// <summary>ORA-01089: immediate shutdown in progress.</summary>
    public const int ShutdownInProgress = 1089;

    // ============================================================
    // Resource and Quota Errors
    // ============================================================

    /// <summary>ORA-04031: unable to allocate shared memory.</summary>
    public const int SharedMemoryError = 4031;

    /// <summary>ORA-01536: space quota exceeded.</summary>
    public const int SpaceQuotaExceeded = 1536;

    /// <summary>ORA-01628: max # extents reached for rollback segment.</summary>
    public const int MaxExtentsReached = 1628;

    /// <summary>ORA-01653: unable to extend table.</summary>
    public const int UnableToExtendTable = 1653;

    // ============================================================
    // Classification Methods
    // ============================================================

    /// <summary>
    /// Determines if the Oracle error is transient and the operation should be retried.
    /// </summary>
    /// <param name="errorCode">The Oracle error code.</param>
    /// <returns><c>true</c> if the error is transient; otherwise, <c>false</c>.</returns>
    public static bool IsTransientError(int errorCode)
    {
        return errorCode switch
        {
            // Lock and concurrency - retry after brief delay
            ResourceBusy => true,
            DeadlockDetected => true,
            WaitTimeoutExpired => true,

            // Connection errors - retry with new connection
            ConnectTimeout => true,
            PacketWriterFailure => true,
            HostUnreachable => true,
            EndOfFileOnChannel => true,
            NotConnected => true,
            ConnectionLostContact => true,
            NotLoggedOn => true,
            DatabaseStartingUp => true,
            ShutdownInProgress => true,

            // Resource errors - may resolve after delay
            SharedMemoryError => true,

            _ => false
        };
    }

    /// <summary>
    /// Determines if the Oracle error is a constraint violation.
    /// </summary>
    /// <param name="errorCode">The Oracle error code.</param>
    /// <returns><c>true</c> if the error is a constraint violation; otherwise, <c>false</c>.</returns>
    public static bool IsConstraintViolation(int errorCode)
    {
        return errorCode is UniqueConstraintViolated or ForeignKeyParentNotFound or ForeignKeyChildRecordFound;
    }

    /// <summary>
    /// Determines if the Oracle error is a connection error that requires reconnection.
    /// </summary>
    /// <param name="errorCode">The Oracle error code.</param>
    /// <returns><c>true</c> if a new connection should be established; otherwise, <c>false</c>.</returns>
    public static bool RequiresReconnection(int errorCode)
    {
        return errorCode is EndOfFileOnChannel or NotConnected or ConnectionLostContact
            or NotLoggedOn or PacketWriterFailure;
    }

    /// <summary>
    /// Determines if the Oracle error is a unique constraint violation,
    /// which may indicate a lock already exists.
    /// </summary>
    /// <param name="ex">The Oracle exception.</param>
    /// <returns><c>true</c> if this is a unique constraint violation; otherwise, <c>false</c>.</returns>
    public static bool IsUniqueConstraintViolation(OracleException ex)
    {
        return ex.Number == UniqueConstraintViolated;
    }

    /// <summary>
    /// Determines if the exception indicates a resource is busy (locked by another session).
    /// </summary>
    /// <param name="ex">The Oracle exception.</param>
    /// <returns><c>true</c> if the resource is busy; otherwise, <c>false</c>.</returns>
    public static bool IsResourceBusy(OracleException ex)
    {
        return ex.Number is ResourceBusy or WaitTimeoutExpired;
    }

    /// <summary>
    /// Determines if the exception indicates a resource is busy (locked by another session).
    /// </summary>
    /// <param name="ex">The exception to check.</param>
    /// <returns><c>true</c> if the resource is busy; otherwise, <c>false</c>.</returns>
    public static bool IsResourceBusy(Exception ex)
    {
        return ex is OracleException oracleEx && IsResourceBusy(oracleEx);
    }

    /// <summary>
    /// Calculates the retry delay using exponential backoff.
    /// </summary>
    /// <param name="attempt">The current attempt number (0-based).</param>
    /// <param name="baseDelay">The base delay between retries.</param>
    /// <param name="maxDelay">The maximum delay cap.</param>
    /// <returns>The calculated delay with jitter.</returns>
    public static TimeSpan CalculateRetryDelay(int attempt, TimeSpan baseDelay, TimeSpan? maxDelay = null)
    {
        // Exponential backoff: baseDelay * 2^attempt
        var exponentialDelay = TimeSpan.FromMilliseconds(
            baseDelay.TotalMilliseconds * Math.Pow(2, attempt));

        // Apply max delay cap
        var cappedDelay = maxDelay.HasValue && exponentialDelay > maxDelay.Value
            ? maxDelay.Value
            : exponentialDelay;

        // Add jitter (±20%) to prevent thundering herd
        var jitterFactor = 0.8 + (Random.Shared.NextDouble() * 0.4);
        return TimeSpan.FromMilliseconds(cappedDelay.TotalMilliseconds * jitterFactor);
    }
}
