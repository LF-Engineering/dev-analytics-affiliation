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
  echo curl -i -s -H "Origin: ${ORIGIN}" -H 'Accept: application/yaml' -XGET "${API_URL}/v1/affiliation/all"
fi

curl -i -s -H "Origin: ${ORIGIN}" -H 'Accept: application/yaml' -XGET "${API_URL}/v1/affiliation/all"
