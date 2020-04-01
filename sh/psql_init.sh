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
cp sh/api_init.sql "$fn"
vim --not-a-term -c "%s/PWD/${APIPASS}/g" -c 'wq!' "$fn"
export PGPASSWORD="${PASS}"
psql -U postgres -h 127.0.0.1 -p 15432 < sh/api_drop.sql
psql -U postgres -h 127.0.0.1 -p 15432 -c "select pg_terminate_backend(pid) from pg_stat_activity where datname = 'dev_analytics'"
psql -U postgres -h 127.0.0.1 -p 15432 -c 'drop database if exists "dev_analytics"'
psql -U postgres -h 127.0.0.1 -p 15432 < "${fn}"
# createdb -U postgres -h 127.0.0.1 -p 15432 dev_analytics
if [ -z "$SQL" ]
then
  pg_restore -U postgres -h 127.0.0.1 -p 15432 -d dev_analytics sh/dev_analytics_prod.dump
else
  psql -U postgres -h 127.0.0.1 -p 15432 dev_analytics < sh/dev_analytics_prod.sql
fi
# psql -U postgres -h 127.0.0.1 -p 15432 dev_analytics -c "insert into access_control_entries(scope, subject, resource, action, effect, extra) select 'odpi/egeria', 'lgryglicki', 'identity', 'manage', 0, '{}'"
psql -U postgres -h 127.0.0.1 -p 15432 dev_analytics -c "insert into access_control_entries(scope, subject, resource, action, effect) select distinct slug, 'lgryglicki', 'identity', 'manage', 0 from projects"
psql -U postgres -h 127.0.0.1 -p 15432 dev_analytics -c "insert into access_control_entries(scope, subject, resource, action, effect) select distinct '/projects/' || slug, 'lgryglicki', 'identity', 'manage', 0 from projects"
psql -U postgres -h 127.0.0.1 -p 15432 dev_analytics -c '\d'
