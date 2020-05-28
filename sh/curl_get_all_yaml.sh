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
  echo curl -i -s -H "Origin: ${ORIGIN}" -H "Authorization: Bearer ${JWT_TOKEN}" -H 'Accept: application/yaml' -XGET "${API_URL}/v1/affiliation/all"
  curl -i -s -H "Origin: ${ORIGIN}" -H "Authorization: Bearer ${JWT_TOKEN}" -H 'Accept: application/yaml' -XGET "${API_URL}/v1/affiliation/all"
else
  curl -s -H "Origin: ${ORIGIN}" -H "Authorization: Bearer ${JWT_TOKEN}" -H 'Accept: application/yaml' -XGET "${API_URL}/v1/affiliation/all"
fi
