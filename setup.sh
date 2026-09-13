#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

if [ "$#" -gt 0 ]; then MONTHS=("$@"); else MONTHS=("2023-01" "2023-02"); fi
TLC_BASE_URL="https://d37ci6vzurychx.cloudfront.net/trip-data"
ZONE_LOOKUP_URL="https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
RAW_DIR="data/raw"
COMPOSE_FILE="assignment_1_batch/docker-compose.yml"

log()  { printf '[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"; }
fail() { log "ERROR: $*" >&2; exit 1; }

set_env_var() {
    local key="$1" value="$2"
    { grep -v "^${key}=" .env || true; echo "${key}=${value}"; } > .env.tmp
    mv .env.tmp .env
}

download() {
    local url="$1" target="$2" tmp="$2.part"
    if [ -s "$target" ]; then
        log "Already present: $target"
        return 0
    fi
    log "Downloading $url"
    if command -v curl > /dev/null 2>&1; then
        curl --fail --location --retry 3 --retry-delay 5 --show-error --progress-bar -o "$tmp" "$url"
    elif command -v wget > /dev/null 2>&1; then
        wget --tries=3 --quiet --show-progress -O "$tmp" "$url"
    else
        fail "curl or wget is required"
    fi
    mv "$tmp" "$target"
}

PYTHON_BIN="$(command -v python3 || command -v python || true)"
[ -n "$PYTHON_BIN" ] || fail "Python 3.10+ is required"
"$PYTHON_BIN" -c 'import sys; sys.exit(0 if sys.version_info >= (3, 10) else 1)' \
    || fail "Python 3.10+ is required (found $("$PYTHON_BIN" --version 2>&1))"
log "Using $("$PYTHON_BIN" --version 2>&1)"

if [ ! -d .venv ]; then
    log "Creating virtual environment .venv"
    "$PYTHON_BIN" -m venv .venv
fi
if [ -f .venv/bin/activate ]; then
    source .venv/bin/activate
else
    source .venv/Scripts/activate
fi
log "Installing dependencies"
python -m pip install --quiet --upgrade pip
python -m pip install --quiet -r requirements.txt

[ -f .env ] || { cp .env.example .env; log "Created .env from .env.example"; }

if command -v docker > /dev/null 2>&1 && docker info > /dev/null 2>&1; then
    log "Starting PostgreSQL (docker compose)"
    docker compose --env-file .env -f "$COMPOSE_FILE" up -d
    for attempt in $(seq 1 30); do
        if docker compose --env-file .env -f "$COMPOSE_FILE" exec -T postgres pg_isready -U postgres -d nyc_taxi > /dev/null 2>&1; then
            log "PostgreSQL is ready (host port from POSTGRES_PORT in .env)"
            break
        fi
        [ "$attempt" -eq 30 ] && fail "PostgreSQL did not become ready in time"
        sleep 2
    done
    set_env_var DB_ENGINE postgres
else
    log "WARNING: Docker daemon not available. Falling back to embedded DuckDB (data/nyc_taxi.duckdb)."
    set_env_var DB_ENGINE duckdb
fi

mkdir -p "$RAW_DIR"
download "$ZONE_LOOKUP_URL" "data/taxi_zone_lookup.csv"
for month in "${MONTHS[@]}"; do
    [[ "$month" =~ ^[0-9]{4}-(0[1-9]|1[0-2])$ ]] || fail "Invalid month '$month' (expected YYYY-MM)"
    download "$TLC_BASE_URL/yellow_tripdata_${month}.parquet" "$RAW_DIR/yellow_tripdata_${month}.parquet"
    [ "$(tail -c 4 "$RAW_DIR/yellow_tripdata_${month}.parquet")" = "PAR1" ] \
        || { rm -f "$RAW_DIR/yellow_tripdata_${month}.parquet"; fail "yellow_tripdata_${month}.parquet is not a valid parquet file"; }
done

log "Creating star schema and seeding dimensions"
python assignment_1_batch/scripts/init_db.py

log "Setup complete. Next steps:"
log "  python assignment_1_batch/etl/pipeline.py --months ${MONTHS[*]}"
log "  streamlit run assignment_1_batch/dashboard/app.py"
