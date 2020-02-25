#!/bin/bash
if ( [ -z "$PASS" ] || [ -z "$APIPASS" ] )
then
  echo "$0: please specify Postgres root password via PASS=..."
  echo "$0: please specify Postgres API password via APIPASS=..."
  exit 1
fi
fn=/tmp/query.sql
function finish {
  rm -f "$fn" 2>/dev/null
}
trap finish EXIT
cp init.sql "$fn"
vim --not-a-term -c "%s/PWD/${APIPASS}/g" -c 'wq!' "$fn"
export PGPASSWORD="${PASS}"
psql -U postgres -h 127.0.0.1 -p 15432 < drop.sql
psql -U postgres -h 127.0.0.1 -p 15432 -c "select pg_terminate_backend(pid) from pg_stat_activity where datname = 'dev_analytics'"
psql -U postgres -h 127.0.0.1 -p 15432 -c 'drop database if exists "dev_analytics"'
psql -U postgres -h 127.0.0.1 -p 15432 < "${fn}"
# createdb -U postgres -h 127.0.0.1 -p 15432 dev_analytics
pg_restore -U postgres -h 127.0.0.1 -p 15432 -d dev_analytics dev_analytics_prod.dump
psql -U postgres -h 127.0.0.1 -p 15432 dev_analytics -c '\d'
