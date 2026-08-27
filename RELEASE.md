# Release guide

Releases are built from an annotated `vMAJOR.MINOR.PATCH` tag on `main`. The tag workflow repeats restore, build, both Oracle integration suites, coverage enforcement, packing, package validation, package inspection, and clean-consumer installation before publishing.

## Prepare 1.0.4

1. Work through a pull request from `codex/release-1.0.4` to `main`.
2. Require all checks from `.github/workflows/ci.yml` to pass.
3. Review package compatibility output against `DevDad.Hangfire.Oracle` 1.0.3 and inspect the generated `.nupkg` and `.snupkg` artifacts.
4. Squash-merge the pull request.
5. Pull the resulting `main` commit and create annotated tag `v1.0.4` on that exact commit.
6. Push the tag once.

The release workflow verifies that the tag and project package versions are identical. It publishes exactly one `.nupkg`; NuGet publishes its associated `.snupkg`. Publishing failures are fatal and duplicates are not ignored. The workflow then creates the GitHub release from `release-notes/1.0.4.md` and attaches both package files.

## Repository configuration

- Secret `NUGET_API_KEY` must be an active NuGet.org API key scoped to `DevDad.Hangfire.Oracle`.
- GitHub Actions must be allowed to create releases with `contents: write` in the tag workflow.
- Branch protection should require the CI workflow before merge.

## Verify after publication

1. Confirm the GitHub release points to the tagged `main` commit and contains one `.nupkg` and one `.snupkg`.
2. Confirm NuGet.org lists version 1.0.4 with the README, license, repository link, dependencies, and symbols.
3. Download the package and verify Source Link resolves source files to the tagged commit.
4. Install from NuGet.org into clean .NET 8 and .NET 10 projects and compile a call to `UseOracleStorage`.
5. Record any publication incident in a GitHub issue; never reuse or move the published tag.

Package signing is intentionally out of scope until a signing certificate and protected signing process are configured.
