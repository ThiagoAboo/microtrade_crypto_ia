param(
    [switch]$Build
)

$ErrorActionPreference = "Stop"
$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
Set-Location $repoRoot

if (-not (Test-Path ".env")) {
    Copy-Item ".env.example" ".env"
}

$args = @("compose", "up", "-d")
if ($Build) {
    $args += "--build"
}

docker @args

