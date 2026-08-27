param(
    [Parameter(Mandatory = $true)]
    [string] $ArtifactsPath
)

$ErrorActionPreference = 'Stop'
$packages = @(Get-ChildItem -LiteralPath $ArtifactsPath -Filter '*.nupkg' | Where-Object Extension -eq '.nupkg')
$symbols = @(Get-ChildItem -LiteralPath $ArtifactsPath -Filter '*.snupkg')

if ($packages.Count -ne 1 -or $symbols.Count -ne 1) {
    throw "Expected exactly one .nupkg and one .snupkg; found $($packages.Count) and $($symbols.Count)."
}

Add-Type -AssemblyName System.IO.Compression.FileSystem
$archive = [IO.Compression.ZipFile]::OpenRead($packages[0].FullName)
try {
    $entries = @($archive.Entries.FullName)
    $required = @(
        'README.md',
        'CHANGELOG.md',
        'UPGRADE.md',
        'LICENSE',
        'lib/net8.0/Hangfire.Oracle.Core.dll',
        'lib/net8.0/Hangfire.Oracle.Core.xml',
        'lib/net10.0/Hangfire.Oracle.Core.dll',
        'lib/net10.0/Hangfire.Oracle.Core.xml',
        'contentFiles/any/any/Sql/Install.sql',
        'contentFiles/any/any/Sql/Uninstall.sql'
    )

    foreach ($entry in $required) {
        if ($entry -notin $entries) {
            throw "Package is missing required entry '$entry'."
        }
    }

    if ($entries -match 'Dapper\.Oracle') {
        throw 'Package unexpectedly contains Dapper.Oracle.'
    }

    $nuspecEntry = $archive.Entries | Where-Object FullName -Like '*.nuspec' | Select-Object -First 1
    $reader = [IO.StreamReader]::new($nuspecEntry.Open())
    try {
        [xml] $nuspec = $reader.ReadToEnd()
    }
    finally {
        $reader.Dispose()
    }

    $metadata = $nuspec.package.metadata
    if ($metadata.id -ne 'DevDad.Hangfire.Oracle' -or $metadata.version -ne '1.0.4') {
        throw "Unexpected package identity '$($metadata.id)' version '$($metadata.version)'."
    }

    if ($metadata.license.InnerText -ne 'MIT' -or $metadata.readme -ne 'README.md') {
        throw 'Package license or README metadata is incomplete.'
    }

    if ([string]::IsNullOrWhiteSpace($metadata.repository.url) -or
        [string]::IsNullOrWhiteSpace($metadata.repository.commit)) {
        throw 'Package repository URL or commit metadata is missing.'
    }

    $dependencyGroups = @($metadata.dependencies.group)
    foreach ($framework in @('net8.0', 'net10.0')) {
        $group = $dependencyGroups | Where-Object targetFramework -EQ $framework
        $dependencies = @($group.dependency)
        foreach ($expectedDependency in @(
            @{ Id = 'Dapper'; Version = '2.1.79' },
            @{ Id = 'Hangfire.Core'; Version = '1.8.24' },
            @{ Id = 'Oracle.ManagedDataAccess.Core'; Version = '23.26.300' }
        )) {
            $actual = $dependencies | Where-Object id -EQ $expectedDependency.Id
            if ($actual.version -ne $expectedDependency.Version) {
                throw "$framework dependency $($expectedDependency.Id) has version '$($actual.version)'."
            }
        }
    }
}
finally {
    $archive.Dispose()
}

$symbolArchive = [IO.Compression.ZipFile]::OpenRead($symbols[0].FullName)
try {
    $symbolEntries = @($symbolArchive.Entries.FullName)
    foreach ($pdb in @(
        'lib/net8.0/Hangfire.Oracle.Core.pdb',
        'lib/net10.0/Hangfire.Oracle.Core.pdb'
    )) {
        if ($pdb -notin $symbolEntries) {
            throw "Symbol package is missing '$pdb'."
        }
    }
}
finally {
    $symbolArchive.Dispose()
}

Write-Host "Verified package $($packages[0].Name) and symbols $($symbols[0].Name)."
