# Redis-only queue benchmark (see backend/cmd/main).
$ErrorActionPreference = "Stop"
$root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
Set-Location (Join-Path $root "backend")
if ($env:REDIS_ADDR) {
  Write-Host "Using REDIS_ADDR=$env:REDIS_ADDR"
}
go run ./cmd/main
