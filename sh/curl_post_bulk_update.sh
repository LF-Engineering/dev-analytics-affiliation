#!/bin/bash
if [ -z "$API_URL" ]
then
  export API_URL="http://127.0.0.1:8080"
fi

if [ ! -z "$DEBUG" ]
then
  echo curl -H 'Accept: application/json' -H 'Content-Type: application/json' -XPOST "${API_URL}/v1/affiliation/bulk_update" -d @sh/example_bulk.json
fi
curl -H 'Accept: application/json' -H 'Content-Type: application/json' -XPOST "${API_URL}/v1/affiliation/bulk_update" -d @sh/example_bulk.json
