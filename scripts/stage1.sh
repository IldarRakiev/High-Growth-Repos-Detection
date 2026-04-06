#!/bin/bash
set -e

echo "============================================"
echo "Stage 1: PostgreSQL + Sqoop"
echo "============================================"

# ---- Configuration ----
DB_HOST="${PGHOST:-localhost}"
DB_PORT="${PGPORT:-5432}"
DB_NAME="${PGDATABASE:-team28}"
DB_USER="${PGUSER:-team28}"
export PGPASSWORD="${PGPASSWORD}"

JDBC_URL="jdbc:postgresql://${DB_HOST}:${DB_PORT}/${DB_NAME}"
HDFS_DIR="/user/team28/project"
DATA_DIR="data"

# ---- Step 1: Create PostgreSQL tables ----
echo ""
echo "[1/5] Creating PostgreSQL tables..."
psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" \
     -f sql/create_tables.sql
echo "  Tables created."

# ---- Step 2: Load dimension table (repositories) ----
echo ""
echo "[2/5] Loading repositories into PostgreSQL..."
psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" \
     -c "\copy repositories FROM '${DATA_DIR}/repositories.csv' WITH (FORMAT csv, HEADER true)"
echo "  Repositories loaded."

# ---- Step 3: Load fact table (events) ----
echo ""
echo "[3/5] Loading events into PostgreSQL..."
psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" \
     -c "\copy events FROM '${DATA_DIR}/events_clean.csv' WITH (FORMAT csv, HEADER true)"
echo "  Events loaded."

# Add indexes after bulk loading for performance
echo "  Creating indexes..."
psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" <<SQL
CREATE INDEX IF NOT EXISTS idx_events_repo_id ON events(repo_id);
CREATE INDEX IF NOT EXISTS idx_events_date    ON events(event_date);
CREATE INDEX IF NOT EXISTS idx_events_type    ON events(event_type);
CREATE INDEX IF NOT EXISTS idx_repos_language ON repositories(language);
SQL
echo "  Indexes created."

# ---- Step 4: Verify loaded data ----
echo ""
echo "[4/5] Verifying PostgreSQL data..."
psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" <<SQL
SELECT 'repositories' AS table_name, COUNT(*) AS row_count FROM repositories
UNION ALL
SELECT 'events',                     COUNT(*)              FROM events;
SQL

# ---- Step 5: Sqoop export to HDFS ----
echo ""
echo "[5/5] Sqoop: exporting PostgreSQL tables to HDFS..."

echo "  Importing repositories..."
sqoop import \
    --connect "$JDBC_URL" \
    --username "$DB_USER" \
    --password "$PGPASSWORD" \
    --table repositories \
    --target-dir "${HDFS_DIR}/repositories" \
    --delete-target-dir \
    --as-textfile \
    -m 1

echo "  Importing events..."
sqoop import \
    --connect "$JDBC_URL" \
    --username "$DB_USER" \
    --password "$PGPASSWORD" \
    --table events \
    --target-dir "${HDFS_DIR}/events" \
    --delete-target-dir \
    --split-by repo_id \
    --as-textfile \
    -m 4

echo ""
echo "  Verifying HDFS data..."
echo "  --- repositories ---"
hadoop fs -ls "${HDFS_DIR}/repositories"
echo "  --- events ---"
hadoop fs -ls "${HDFS_DIR}/events"

echo ""
echo "============================================"
echo "Stage 1 complete!"
echo "  PostgreSQL: repositories + events loaded"
echo "  HDFS:       ${HDFS_DIR}/repositories"
echo "              ${HDFS_DIR}/events"
echo "============================================"
