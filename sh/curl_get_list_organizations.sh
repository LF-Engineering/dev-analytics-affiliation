#!/bin/bash
. ./sh/shared.sh
q=$(rawurlencode "${2}")
rows=$(rawurlencode "${3}")
page=$(rawurlencode "${4}")

if [ ! -z "$DEBUG" ]
then
  echo curl -i -s -H "Origin: https://test.lfanalytics.io" -H "Authorization: Bearer ${JWT_TOKEN}" -XGET "${API_URL}/v1/affiliation/${project}/list_organizations?q=${q}&rows=${rows}&page=${page}"
  curl -i -s -H "Origin: ${ORIGIN}" -H "Authorization: Bearer ${JWT_TOKEN}" -XGET "${API_URL}/v1/affiliation/${project}/list_organizations?q=${q}&rows=${rows}&page=${page}"
else
  curl -s -H "Origin: ${ORIGIN}" -H "Authorization: Bearer ${JWT_TOKEN}" -XGET "${API_URL}/v1/affiliation/${project}/list_organizations?q=${q}&rows=${rows}&page=${page}"
fi

