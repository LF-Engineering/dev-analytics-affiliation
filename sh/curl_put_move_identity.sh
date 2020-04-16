#!/bin/bash
. ./sh/shared.sh
if [ -z "$2" ]
then
  echo "$0: please specify from identity id as a 2nd arg"
  exit 3
fi
if [ -z "$3" ]
then
  echo "$0: please specify to uidentity uuid as a 3rd arg"
  exit 4
fi
from_id=$(rawurlencode "${2}")
to_uuid=$(rawurlencode "${3}")
ar="true"
if [ "$4" = "0" ]
then
  ar="false"
fi

if [ ! -z "$DEBUG" ]
then
  echo "$project $from_id $to_uuid"
  echo curl -s -H "Origin: ${ORIGIN}" -H "Authorization: Bearer ${JWT_TOKEN}" -XPUT "${API_URL}/v1/affiliation/${project}/move_identity/${from_id}/${to_uuid}?archive=${ar}"
fi
curl -s -H "Origin: ${ORIGIN}" -H "Authorization: Bearer ${JWT_TOKEN}" -XPUT "${API_URL}/v1/affiliation/${project}/move_identity/${from_id}/${to_uuid}?archive=${ar}"
