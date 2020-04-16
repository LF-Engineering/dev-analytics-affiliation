#!/bin/bash
. ./sh/shared.sh
if [ -z "$2" ]
then
  echo "$0: please specify organization name as a 2nd arg"
  exit 2
fi
orgName=$(rawurlencode "${2}")

if [ ! -z "$DEBUG" ]
then
  echo curl -i -s -H "Origin: ${ORIGIN}" -H "Authorization: Bearer ${JWT_TOKEN}" -XPOST "${API_URL}/v1/affiliation/${project}/add_organization/${orgName}"
  curl -i -s -H "Origin: ${ORIGIN}" -H "Authorization: Bearer ${JWT_TOKEN}" -XPOST "${API_URL}/v1/affiliation/${project}/add_organization/${orgName}"
else
  curl -s -H "Origin: ${ORIGIN}" -H "Authorization: Bearer ${JWT_TOKEN}" -XPOST "${API_URL}/v1/affiliation/${project}/add_organization/${orgName}"
fi
