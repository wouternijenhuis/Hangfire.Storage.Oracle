param(
    [Parameter(Mandatory = $true)]
    [string] $Tag
)

$ErrorActionPreference = 'Stop'
if ($Tag -notmatch '^v(?<Version>\d+\.\d+\.\d+)$') {
    throw "Release tag '$Tag' must be a stable semantic version such as v1.0.4."
}

[xml] $properties = Get-Content -Raw -LiteralPath 'Directory.Build.props'
$versionNode = $properties.Project.PropertyGroup.Version | Select-Object -First 1
$packageVersionNode = $properties.Project.PropertyGroup.PackageVersion | Select-Object -First 1
$expected = $Matches.Version
if ($versionNode -ne $expected -or $packageVersionNode -ne $expected) {
    throw "Tag version '$expected' does not match Version '$versionNode' and PackageVersion '$packageVersionNode'."
}

$releaseNotes = "release-notes/$expected.md"
if (-not (Test-Path -LiteralPath $releaseNotes)) {
    throw "Missing release notes '$releaseNotes'."
}

Write-Host "Release tag, package metadata, and release notes all identify version $expected."
