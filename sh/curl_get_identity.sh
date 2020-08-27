#!/bin/bash
export SKIP_PROJECT=1
. ./sh/shared.sh
if [ -z "$1" ]
then
  echo "$0: please specify profile UUID as a 2nd arg"
  exit 2
fi
uuid=$(rawurlencode "${1}")

if [ ! -z "$DEBUG" ]
then
  echo curl -i -s -H "Origin: ${ORIGIN}" -H "Authorization: Bearer ${JWT_TOKEN}" -XGET "${API_URL}/v1/affiliation/get_identity/${uuid}"
  curl -i -s -H "Origin: ${ORIGIN}" -H "Authorization: Bearer ${JWT_TOKEN}" -XGET "${API_URL}/v1/affiliation/get_identity/${uuid}"
else
  curl -s -H "Origin: ${ORIGIN}" -H "Authorization: Bearer ${JWT_TOKEN}" -XGET "${API_URL}/v1/affiliation/get_identity/${uuid}"
fi
