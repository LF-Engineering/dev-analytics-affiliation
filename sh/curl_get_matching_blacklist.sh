#!/bin/bash
. ./sh/shared.sh
q=$(rawurlencode "${2}")
rows=$(rawurlencode "${3}")
page=$(rawurlencode "${4}")

if [ ! -z "$DEBUG" ]
then
  echo curl -s -H "Origin: ${ORIGIN}" -H "Authorization: Bearer ${JWT_TOKEN}" -XGET "${API_URL}/v1/affiliation/${project}/matching_blacklist?q=${q}&rows=${rows}&page=${page}"
fi

curl -s -H "Origin: ${ORIGIN}" -H "Authorization: Bearer ${JWT_TOKEN}" -XGET "${API_URL}/v1/affiliation/${project}/matching_blacklist?q=${q}&rows=${rows}&page=${page}"
