#!/bin/bash
. ./sh/shared.sh
orgID=$(rawurlencode "${2}")
q=$(rawurlencode "${3}")
rows=$(rawurlencode "${4}")
page=$(rawurlencode "${5}")

if [ ! -z "$DEBUG" ]
then
  echo curl -i -s -H "Origin: ${ORIGIN}" -H "Authorization: Bearer ${JWT_TOKEN}" -XGET "${API_URL}/v1/affiliation/${project}/list_domains?orgID=${orgID}&q=${q}&rows=${rows}&page=${page}"
  curl -i -s -H "Origin: ${ORIGIN}" -H "Authorization: Bearer ${JWT_TOKEN}" -XGET "${API_URL}/v1/affiliation/${project}/list_domains?orgID=${orgID}&q=${q}&rows=${rows}&page=${page}"
else
  curl -s -H "Origin: ${ORIGIN}" -H "Authorization: Bearer ${JWT_TOKEN}" -XGET "${API_URL}/v1/affiliation/${project}/list_domains?orgID=${orgID}&q=${q}&rows=${rows}&page=${page}"
fi
