param(
    [string[]]$Months = @("2023-01", "2023-02")
)
$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

$TlcBaseUrl = "https://d37ci6vzurychx.cloudfront.net/trip-data"
$ZoneLookupUrl = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
$ComposeFile = "assignment_1_batch/docker-compose.yml"

function Log($Message) { Write-Host "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] $Message" }

function Set-EnvVar($Key, $Value) {
    $lines = @(Get-Content .env | Where-Object { $_ -notmatch "^$Key=" })
    $lines += "$Key=$Value"
    Set-Content -Path .env -Value $lines -Encoding utf8
}

function Download($Url, $Target) {
    if ((Test-Path $Target) -and ((Get-Item $Target).Length -gt 0)) { Log "Already present: $Target"; return }
    Log "Downloading $Url"
    & curl.exe --fail --location --retry 3 --retry-delay 5 --show-error -o "$Target.part" $Url
    if ($LASTEXITCODE -ne 0) { throw "Download failed: $Url" }
    Move-Item -Force "$Target.part" $Target
}

$python = Get-Command python -ErrorAction SilentlyContinue
if (-not $python) { throw "Python 3.10+ is required" }
& python -c "import sys; sys.exit(0 if sys.version_info >= (3, 10) else 1)"
if ($LASTEXITCODE -ne 0) { throw "Python 3.10+ is required" }

if (-not (Test-Path .venv)) { Log "Creating virtual environment .venv"; python -m venv .venv }
$venvPython = ".\.venv\Scripts\python.exe"
Log "Installing dependencies"
& $venvPython -m pip install --quiet --upgrade pip
& $venvPython -m pip install --quiet -r requirements.txt
if ($LASTEXITCODE -ne 0) { throw "pip install failed" }

if (-not (Test-Path .env)) { Copy-Item .env.example .env; Log "Created .env from .env.example" }

$dockerOk = $false
try { docker info *> $null; $dockerOk = ($LASTEXITCODE -eq 0) } catch { $dockerOk = $false }
if ($dockerOk) {
    Log "Starting PostgreSQL (docker compose)"
    docker compose --env-file .env -f $ComposeFile up -d
    $ready = $false
    for ($i = 0; $i -lt 30; $i++) {
        docker compose --env-file .env -f $ComposeFile exec -T postgres pg_isready -U postgres -d nyc_taxi *> $null
        if ($LASTEXITCODE -eq 0) { $ready = $true; break }
        Start-Sleep -Seconds 2
    }
    if (-not $ready) { throw "PostgreSQL did not become ready in time" }
    Set-EnvVar "DB_ENGINE" "postgres"
} else {
    Log "WARNING: Docker daemon not available. Falling back to embedded DuckDB."
    Set-EnvVar "DB_ENGINE" "duckdb"
}

New-Item -ItemType Directory -Force data/raw | Out-Null
Download $ZoneLookupUrl "data/taxi_zone_lookup.csv"
foreach ($month in $Months) {
    if ($month -notmatch '^\d{4}-(0[1-9]|1[0-2])$') { throw "Invalid month '$month' (expected YYYY-MM)" }
    Download "$TlcBaseUrl/yellow_tripdata_$month.parquet" "data/raw/yellow_tripdata_$month.parquet"
}

Log "Creating star schema and seeding dimensions"
& $venvPython assignment_1_batch/scripts/init_db.py
if ($LASTEXITCODE -ne 0) { throw "init_db.py failed" }

Log "Setup complete. Next steps:"
Log "  .\.venv\Scripts\python assignment_1_batch/etl/pipeline.py --months $($Months -join ' ')"
Log "  .\.venv\Scripts\streamlit run assignment_1_batch/dashboard/app.py"
