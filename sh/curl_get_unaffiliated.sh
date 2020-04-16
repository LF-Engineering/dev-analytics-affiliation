#!/bin/bash
. ./sh/shared.sh
rows=$(rawurlencode "${2}")
page=$(rawurlencode "${3}")

if [ ! -z "$DEBUG" ]
then
  echo curl -s -H "Origin: ${ORIGIN}" -H "Authorization: Bearer ${JWT_TOKEN}" -XGET "${API_URL}/v1/affiliation/${project}/unaffiliated?rows=${rows}&page=${page}"
fi

curl -s -H "Origin: ${ORIGIN}" -H "Authorization: Bearer ${JWT_TOKEN}" -XGET "${API_URL}/v1/affiliation/${project}/unaffiliated?rows=${rows}&page=${page}"
