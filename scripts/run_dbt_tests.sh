#!/usr/bin/env bash

# Exit immediately if a command exits with a non-zero status
set -e

# Export required PostgreSQL Environment Variables for dbt profiles.yml
export PGHOST=localhost
export PGPORT=5432
export PGUSER=postgres
export PGPASSWORD=postgres
export PGDATABASE=postgres

echo "Note: This script requires a local PostgreSQL instance running on port 5432 with the credentials above."
echo "If you have docker installed and working with user permissions, you can run:"
echo 'docker run --name postgres -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=postgres -p 5432:5432 -d postgres:15'
echo ""

echo "Waiting for PostgreSQL to become ready (optional if already running)..."
sleep 2

echo "Seeding mock data for local CI..."
psql -h $PGHOST -U $PGUSER -d $PGDATABASE -c "CREATE SCHEMA IF NOT EXISTS bronze;" || true
psql -h $PGHOST -U $PGUSER -d $PGDATABASE -c "
CREATE TABLE IF NOT EXISTS bronze.bronze_livertox_raw (
    coreason_id TEXT,
    uid TEXT,
    raw_data JSONB,
    extracted_blocks JSONB,
    ingestion_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);" || true

psql -h $PGHOST -U $PGUSER -d $PGDATABASE -c "
INSERT INTO bronze.bronze_livertox_raw (coreason_id, uid, raw_data, extracted_blocks)
VALUES (
    '123',
    '123',
    '{}'::jsonb,
    '{\"hepatotoxicity_summary\": \"Test Summary\", \"likelihood_score\": \"Likelihood score: A\", \"mechanism_of_injury\": \"Test Mechanism\"}'::jsonb
);" || true

echo "Running dbt dependencies via uvx with Python 3.12..."
uvx --from dbt-core --python 3.12 --with dbt-postgres dbt deps --project-dir src/coreason_etl_liver_tox/dbt_project

echo "Running dbt models via uvx with Python 3.12..."
uvx --from dbt-core --python 3.12 --with dbt-postgres dbt run --project-dir src/coreason_etl_liver_tox/dbt_project --profiles-dir src/coreason_etl_liver_tox/dbt_project

echo "Running dbt tests via uvx with Python 3.12..."
uvx --from dbt-core --python 3.12 --with dbt-postgres dbt test --project-dir src/coreason_etl_liver_tox/dbt_project --profiles-dir src/coreason_etl_liver_tox/dbt_project

echo "Success! All dbt models ran and tests passed."
