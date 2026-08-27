# Implementation notes

DevDad.Hangfire.Oracle implements Hangfire's storage abstractions on Oracle Database 19c or later.

## Schema lifecycle

`OracleSchemaManager` qualifies validated unquoted identifiers, parses embedded SQL without splitting inside strings or comments, applies known idempotent DDL, migrates schema version 1 to version 2, and validates the complete required object set. Unexpected DDL failures are never suppressed. The schema version is stored in the Hangfire hash table.

## Queue ownership

One internal queue implementation performs Oracle-compatible row locking. A dequeue assigns a unique fetch token and timestamp. Remove, requeue, and lease renewal include that token in their predicate, so a stale worker cannot acknowledge a replacement owner's fetch. Disposing an uncompleted fetched job requeues it. Abandoned fetches become eligible after the configured invisibility timeout.

The public `OracleJobQueue` type remains as a compatibility wrapper around the same implementation.

## Counters and cleanup

Counter aggregation locks a deterministic batch, merges each value into the aggregated table, and deletes exactly the locked rows in one transaction. Reads sum pending and aggregated values. Cleanup includes expired raw counters and uses bounded batches.

## Distributed locks

Table-backed locks store owner and expiry values, renew active leases, replace expired owners atomically, and release only when owner identity still matches. `UseDbmsLock` selects Oracle application locks and requires the corresponding database privilege.

## Time, SQL, and diagnostics

Storage timestamps consistently follow `UseUtcTime`. Commands use configured timeouts, identifiers cannot inject SQL, queue pagination uses Oracle-compatible syntax, and transient retry is restricted to classified Oracle failures. Monitoring APIs normalize Hangfire state names and enforce dashboard list limits.

## Compatibility

The package targets `net8.0` and `net10.0`. NuGet package validation compares its public surface with 1.0.3. Internal restructuring must not remove the existing public package, assembly, namespace, or types during the 1.x line.
