#!/bin/bash
. ./sh/shared.sh
if [ -z "$2" ]
then
  echo "$0: please specify profile uuid as a 2nd arg"
  exit 2
fi
if [ -z "$3" ]
then
    echo "$0: please specify organization name (must exist) as a 3rd arg"
  exit 3
fi
uuid=$(rawurlencode "${2}")
orgName=$(rawurlencode "${3}")

if [ ! -z "$DEBUG" ]
then
  echo curl -i -s -H "Origin: ${ORIGIN}" -H "Authorization: Bearer ${JWT_TOKEN}" -XPUT "${API_URL}/v1/affiliation/${project}/merge_enrollments/${uuid}/${orgName}"
  curl -i -s -H "Origin: ${ORIGIN}" -H "Authorization: Bearer ${JWT_TOKEN}" -XPUT "${API_URL}/v1/affiliation/${project}/merge_enrollments/${uuid}/${orgName}"
else
  curl -s -H "Origin: ${ORIGIN}" -H "Authorization: Bearer ${JWT_TOKEN}" -XPUT "${API_URL}/v1/affiliation/${project}/merge_enrollments/${uuid}/${orgName}"
fi
