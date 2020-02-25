#!/bin/bash
if ( [ -z "$USR" ] || [ -z "$PASS" ] )
then
  echo "$0: please specify user via USR=..."
  echo "$0: please specify password via PASS=..."
  exit 1
fi
PGPASSWORD="${PASS}" psql -U "${USR}" -h 127.0.0.1 -p 15432 dev_analytics
