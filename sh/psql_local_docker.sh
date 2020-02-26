#!/bin/bash
if [ -z "$PASS" ]
then
  echo "$0: please specify Postgres password via PASS=..."
  exit 1
fi
docker run -p 15432:5432 -e POSTGRES_PASSWORD="${PASS}" postgres
