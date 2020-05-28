#!/bin/bash
if [ -z "$JWT_TOKEN" ]
then
  echo "$0: please sepcify your JWT token via JWT_TOKEN=..."
  exit 1
fi

if [ -z "$API_URL" ]
then
  export API_URL="http://127.0.0.1:8080"
fi

if [ -z "$ORIGIN" ]
then
  export ORIGIN="https://insights.test.platform.linuxfoundation.org"
fi

if [ ! -z "$DEBUG" ]
then
  echo curl -i -s -H "Origin: ${ORIGIN}" -H "Authorization: Bearer ${JWT_TOKEN}" -H 'Accept: application/json' -H 'Content-Type: application/json' -XPOST "${API_URL}/v1/affiliation/bulk_update" -d @sh/example_bulk.json
  curl -i -s -H 'Accept: application/json' -H "Origin: ${ORIGIN}" -H 'Content-Type: application/json' -H "Authorization: Bearer ${JWT_TOKEN}" -XPOST "${API_URL}/v1/affiliation/bulk_update" -d @sh/example_bulk.json
else
  curl -s -H 'Accept: application/json' -H "Origin: ${ORIGIN}" -H 'Content-Type: application/json' -H "Authorization: Bearer ${JWT_TOKEN}" -XPOST "${API_URL}/v1/affiliation/bulk_update" -d @sh/example_bulk.json
fi
