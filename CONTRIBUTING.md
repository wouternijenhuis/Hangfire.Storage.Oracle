# Contributing

Thank you for improving DevDad.Hangfire.Oracle.

## Prerequisites

- .NET SDK 10.0.400
- Docker or another Oracle Database 23 Free instance
- An Oracle application user allowed to create and alter its Hangfire objects

## Build and test

```powershell
dotnet restore Hangfire.Oracle.sln --locked-mode
dotnet format Hangfire.Oracle.sln --verify-no-changes --no-restore
dotnet build Hangfire.Oracle.sln -c Release --no-restore
```

Set `ORACLE_TEST_CONNECTION_STRING`, then execute both test applications:

```powershell
dotnet run --project tests/Hangfire.Oracle.Core.Tests -c Release -f net8.0 --no-build -- --minimum-expected-tests 40
dotnet run --project tests/Hangfire.Oracle.Core.Tests -c Release -f net10.0 --no-build -- --minimum-expected-tests 40
```

The integration fixture creates and removes uniquely prefixed objects in the configured user; never point it at a production schema.

## Pull requests

- Add focused tests for behavior changes and regression tests for defects.
- Preserve the package ID, assembly, namespace, and existing public API in 1.x changes.
- Keep SQL compatible with Oracle 19c.
- Update the changelog and upgrade guide for user-visible changes.
- Do not modify generated lock files manually; run restore and commit intentional updates.
- Ensure build, analyzer, formatting, audit, coverage, pack, and smoke-install checks pass.

By contributing, you agree that your contribution is licensed under the repository's MIT license.
