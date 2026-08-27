param(
    [Parameter(Mandatory = $true)]
    [string] $Path,
    [double] $MinimumLineRate = 0.70,
    [double] $MinimumBranchRate = 0.50
)

$ErrorActionPreference = 'Stop'
[xml] $report = Get-Content -Raw -LiteralPath $Path
$lineRate = [double]::Parse($report.coverage.'line-rate', [Globalization.CultureInfo]::InvariantCulture)
$branchRate = [double]::Parse($report.coverage.'branch-rate', [Globalization.CultureInfo]::InvariantCulture)

Write-Host ('Coverage: {0:P2} lines, {1:P2} branches' -f $lineRate, $branchRate)
if ($lineRate -lt $MinimumLineRate -or $branchRate -lt $MinimumBranchRate) {
    throw ('Coverage gate failed. Required at least {0:P0} lines and {1:P0} branches.' -f $MinimumLineRate, $MinimumBranchRate)
}
