#!/bin/bash
. ./sh/shared.sh
if [ -z "$2" ]
then
  echo "$0: please specify unique identity uuid as a 2nd arg"
  exit 2
fi
uuid=$(rawurlencode "${2}")

if [ ! -z "$DEBUG" ]
then
  echo curl -s -H "Origin: ${ORIGIN}" -H "Authorization: Bearer ${JWT_TOKEN}" -XPOST "${API_URL}/v1/affiliation/${project}/add_unique_identity/${uuid}"
fi

curl -s -H "Origin: ${ORIGIN}" -H "Authorization: Bearer ${JWT_TOKEN}" -XPOST "${API_URL}/v1/affiliation/${project}/add_unique_identity/${uuid}"
