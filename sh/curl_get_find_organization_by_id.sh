#!/bin/bash
. ./sh/shared.sh
if [ -z "$2" ]
then
  echo "$0: please specify organization ID as a 2nd arg"
  exit 2
fi
orgID=$(rawurlencode "${2}")

if [ ! -z "$DEBUG" ]
then
  echo curl -i -s -H "Origin: ${ORIGIN}" -H "Authorization: Bearer ${JWT_TOKEN}" -XGET "${API_URL}/v1/affiliation/${project}/find_organization_by_id/${orgID}"
  curl -i -s -H "Origin: ${ORIGIN}" -H "Authorization: Bearer ${JWT_TOKEN}" -XGET "${API_URL}/v1/affiliation/${project}/find_organization_by_id/${orgID}"
else
  curl -s -H "Origin: ${ORIGIN}" -H "Authorization: Bearer ${JWT_TOKEN}" -XGET "${API_URL}/v1/affiliation/${project}/find_organization_by_id/${orgID}"
fi
