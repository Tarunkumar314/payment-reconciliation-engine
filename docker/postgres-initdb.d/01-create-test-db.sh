#!/bin/bash
# This script runs automatically on a FRESH postgres container (empty data volume).
# For an existing volume, create the test DB manually:
#   docker exec reconcile_postgres psql -U reconcile_user -c "CREATE DATABASE reconcile_test_db OWNER reconcile_user;"
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    SELECT 'CREATE DATABASE reconcile_test_db OWNER $POSTGRES_USER'
    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'reconcile_test_db')\gexec
EOSQL
