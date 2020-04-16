#!/bin/bash
if [ -z "$API_URL" ]
then
  export API_URL="http://127.0.0.1:8080"
fi

if [ -z "$ORIGIN" ]
then
  export ORIGIN="https://test.lfanalytics.io"
fi

if [ ! -z "$DEBUG" ]
then
  echo curl -i -s -H "Origin: ${ORIGIN}" -H 'Accept: application/json' -H 'Content-Type: application/json' -XPOST "${API_URL}/v1/affiliation/bulk_update" -d @sh/example_bulk.json
  curl -i -s -H 'Accept: application/json' -H "Origin: ${ORIGIN}" -H 'Content-Type: application/json' -XPOST "${API_URL}/v1/affiliation/bulk_update" -d @sh/example_bulk.json
else
  curl -s -H 'Accept: application/json' -H "Origin: ${ORIGIN}" -H 'Content-Type: application/json' -XPOST "${API_URL}/v1/affiliation/bulk_update" -d @sh/example_bulk.json
fi
