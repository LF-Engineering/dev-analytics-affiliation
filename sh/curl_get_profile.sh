#!/bin/bash
. ./sh/shared.sh
if [ -z "$2" ]
then
  echo "$0: please specify profile UUID as a 2nd arg"
  exit 2
fi
uuid=$(rawurlencode "${2}")

if [ ! -z "$DEBUG" ]
then
  echo curl -i -s -H "Origin: ${ORIGIN}" -H "Authorization: Bearer ${JWT_TOKEN}" -XGET "${API_URL}/v1/affiliation/${project}/get_profile/${uuid}"
  curl -i -s -H "Origin: ${ORIGIN}" -H "Authorization: Bearer ${JWT_TOKEN}" -XGET "${API_URL}/v1/affiliation/${project}/get_profile/${uuid}"
else
  curl -s -H "Origin: ${ORIGIN}" -H "Authorization: Bearer ${JWT_TOKEN}" -XGET "${API_URL}/v1/affiliation/${project}/get_profile/${uuid}"
fi
