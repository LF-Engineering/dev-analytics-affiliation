#!/bin/bash
. ./sh/shared.sh
if [ ! -z "$DEBUG" ]
then
  echo curl -i -s -H "Origin: ${ORIGIN}" -H "Authorization: Bearer ${JWT_TOKEN}" -H 'Accept: application/json' -H 'Content-Type: application/json' -XPOST "${API_URL}/v1/affiliation/${project}/add_identities" -d @sh/example_add_identities.json
  curl -i -s -H 'Accept: application/json' -H "Origin: ${ORIGIN}" -H 'Content-Type: application/json' -H "Authorization: Bearer ${JWT_TOKEN}" -XPOST "${API_URL}/v1/affiliation/${project}/add_identities" -d @sh/example_add_identities.json
else
  curl -s -H 'Accept: application/json' -H "Origin: ${ORIGIN}" -H 'Content-Type: application/json' -H "Authorization: Bearer ${JWT_TOKEN}" -XPOST "${API_URL}/v1/affiliation/${project}/add_identities" -d @sh/example_add_identities.json
fi
