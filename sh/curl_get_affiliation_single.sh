#!/bin/bash
. ./sh/shared.sh
if [ -z "$2" ]
then
  echo "$0: please specify UUID as a 2nd arg"
  exit 2
fi
if [ -z "$3" ]
then
  echo "$0: please specify dt as a 3rd arg (format 2015-05-05T15:15[:05Z])"
  exit 3
fi
uuid=$(rawurlencode "${2}")
dt=$(rawurlencode "${3}")

if [ ! -z "$DEBUG" ]
then
  echo curl -i -s -H "Origin: ${ORIGIN}" -H "Authorization: Bearer ${JWT_TOKEN}" -XGET "${API_URL}/v1/affiliation/${project}/single/${uuid}/${dt}"
  curl -i -s -H "Origin: ${ORIGIN}" -H "Authorization: Bearer ${JWT_TOKEN}" -XGET "${API_URL}/v1/affiliation/${project}/single/${uuid}/${dt}"
else
  curl -s -H "Origin: ${ORIGIN}" -H "Authorization: Bearer ${JWT_TOKEN}" -XGET "${API_URL}/v1/affiliation/${project}/single/${uuid}/${dt}"
fi
