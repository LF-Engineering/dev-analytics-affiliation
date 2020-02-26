#!/bin/bash
if [ -z "$PASS" ]
then
  echo "$0: please specify Postgres root password via PASS=..."
  exit 1
fi
PGPASSWORD="${PASS}" psql -U postgres -h 127.0.0.1 -p 15432 dev_analytics
