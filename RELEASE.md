# Release Process

This repository uses GitHub Actions to automate the release process.

## Creating a Release

To create a new release, follow these steps:

1. **Ensure all changes are merged** to the main branch and all tests pass.

2. **Create and push a version tag**:
   ```bash
   git tag v1.0.0
   git push origin v1.0.0
   ```
   
   Use semantic versioning (MAJOR.MINOR.PATCH):
   - **MAJOR**: Breaking changes
   - **MINOR**: New features (backward compatible)
   - **PATCH**: Bug fixes (backward compatible)

3. **The GitHub Actions workflow will automatically**:
   - Build the project for both .NET 8.0 and .NET 10.0
   - Run all tests
   - Create a NuGet package
   - Generate release notes from commits
   - Create a GitHub Release with the package attached
   - Publish to NuGet.org (if configured)

## Pre-release Versions

For pre-release versions, use a hyphen followed by a pre-release identifier:
```bash
git tag v1.0.0-beta.1
git push origin v1.0.0-beta.1
```

Pre-release versions will be marked as "Pre-release" on GitHub and will NOT be automatically published to NuGet.org.

## NuGet.org Publishing

To enable automatic publishing to NuGet.org:

1. Create a NuGet API key at https://www.nuget.org/account/apikeys
2. Add the API key as a repository secret:
   - Go to repository Settings → Secrets and variables → Actions
   - Click "New repository secret"
   - Name: `NUGET_API_KEY`
   - Value: Your NuGet API key
   - Click "Add secret"

The workflow will automatically publish stable releases (without pre-release suffixes) to NuGet.org.

## Manual Release

If you need to create a package manually:

```bash
# Build the project
dotnet build --configuration Release

# Pack the NuGet package
dotnet pack src/Hangfire.Oracle.Core/Hangfire.Oracle.Core.csproj \
  --configuration Release \
  --output ./artifacts \
  /p:PackageVersion=1.0.0

# The package will be created in ./artifacts/
```

## Workflow Configuration

The release workflow is defined in `.github/workflows/release.yml` and runs when:
- A tag matching the pattern `v*.*.*` is pushed (e.g., v1.0.0, v2.1.3)

### Workflow Features

- ✅ Multi-framework build (.NET 8.0 and .NET 10.0)
- ✅ Automated testing
- ✅ NuGet package creation (with symbols)
- ✅ GitHub Release creation
- ✅ Auto-generated release notes
- ✅ Optional NuGet.org publishing
- ✅ Artifact retention (90 days)

## Troubleshooting

### Workflow fails with "Package already exists"

This is normal if you're re-running a workflow for an existing tag. The NuGet push step has `continue-on-error: true` to handle this gracefully.

### Release doesn't appear

1. Check the Actions tab for workflow errors
2. Verify the tag matches the pattern `v*.*.*`
3. Ensure you have write permissions to the repository

### NuGet publishing fails

1. Verify the `NUGET_API_KEY` secret is configured
2. Check that the API key has permission to publish packages
3. Ensure the package version doesn't already exist on NuGet.org
