$ErrorActionPreference = "Stop"
$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
Set-Location $repoRoot

$env:PYTHONPATH = Join-Path $repoRoot "src"
uvicorn api.main:create_app --factory --host 0.0.0.0 --port 8000

