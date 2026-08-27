param(
    [Parameter(Mandatory = $true)]
    [string] $ArtifactsPath
)

$ErrorActionPreference = 'Stop'
$package = @(Get-ChildItem -LiteralPath $ArtifactsPath -Filter '*.nupkg' | Where-Object Extension -eq '.nupkg')
if ($package.Count -ne 1) {
    throw "Expected one package for smoke testing; found $($package.Count)."
}

$packageRoot = (Resolve-Path -LiteralPath $ArtifactsPath).Path
$smokeRoot = Join-Path ([IO.Path]::GetTempPath()) "HangfireOracleSmoke-$([Guid]::NewGuid().ToString('N'))"
foreach ($framework in @('net8.0', 'net10.0')) {
    $projectRoot = Join-Path $smokeRoot $framework
    dotnet new console --output $projectRoot --force --no-restore
    if ($LASTEXITCODE -ne 0) { throw "Unable to create the $framework smoke project." }

    $project = Get-ChildItem -LiteralPath $projectRoot -Filter '*.csproj' | Select-Object -First 1
    [xml] $projectXml = Get-Content -Raw -LiteralPath $project.FullName
    $projectXml.Project.PropertyGroup.TargetFramework = $framework
    $projectXml.Save($project.FullName)
    dotnet add $project.FullName package DevDad.Hangfire.Oracle --version 1.0.4 --no-restore
    if ($LASTEXITCODE -ne 0) { throw "Unable to add the package to the $framework smoke project." }

    $program = @'
using Hangfire;
using Hangfire.Oracle.Core;

IGlobalConfiguration configuration = GlobalConfiguration.Configuration;
_ = configuration.UseOracleStorage("User Id=user;Password=password;Data Source=example");
'@
    Set-Content -LiteralPath (Join-Path $projectRoot 'Program.cs') -Value $program -Encoding utf8NoBOM
    dotnet restore $project.FullName "-p:RestoreAdditionalProjectSources=$packageRoot"
    if ($LASTEXITCODE -ne 0) { throw "Unable to restore the $framework smoke project." }

    dotnet build $project.FullName --configuration Release --no-restore --nologo
    if ($LASTEXITCODE -ne 0) { throw "Unable to build the $framework smoke project." }
}

Write-Host 'The package installs and compiles in clean .NET 8 and .NET 10 consumers.'
