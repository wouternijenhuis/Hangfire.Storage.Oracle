param(
    [Parameter(Mandatory = $true)]
    [string] $ArtifactsPath,
    [Parameter(Mandatory = $true)]
    [string] $ApiKey
)

$ErrorActionPreference = 'Stop'
if ([string]::IsNullOrWhiteSpace($ApiKey)) {
    throw 'NUGET_API_KEY is not configured.'
}

$packages = @(Get-ChildItem -LiteralPath $ArtifactsPath -Filter '*.nupkg' | Where-Object Extension -eq '.nupkg')
$symbols = @(Get-ChildItem -LiteralPath $ArtifactsPath -Filter '*.snupkg')
if ($packages.Count -ne 1 -or $symbols.Count -ne 1) {
    throw "Expected exactly one .nupkg and one .snupkg before publication; found $($packages.Count) and $($symbols.Count)."
}

dotnet nuget push $packages[0].FullName --api-key $ApiKey --source https://api.nuget.org/v3/index.json
if ($LASTEXITCODE -ne 0) {
    throw "NuGet.org rejected $($packages[0].Name)."
}
