#!/bin/bash
. ./sh/shared.sh
if [ -z "$2" ]
then
  echo "$0: please specify organization ID as a 2nd arg"
  exit 3
fi
if [ -z "$3" ]
then
  echo "$0: please specify organization name as a 3rd arg"
  exit 4
fi
orgID=$(rawurlencode "${2}")
orgName=$(rawurlencode "${3}")

if [ ! -z "$DEBUG" ]
then
  echo curl -s -H "Origin: ${ORIGIN}" -H "Authorization: Bearer ${JWT_TOKEN}" -XPUT "${API_URL}/v1/affiliation/${project}/edit_organization/${orgID}/${orgName}"
fi

curl -s -H "Origin: ${ORIGIN}" -H "Authorization: Bearer ${JWT_TOKEN}" -XPUT "${API_URL}/v1/affiliation/${project}/edit_organization/${orgID}/${orgName}"
