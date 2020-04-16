#!/bin/bash
. ./sh/shared.sh
if [ -z "$2" ]
then
  echo "$0: please specify identity id as a 2nd arg"
  exit 2
fi
id=$(rawurlencode "${2}")

if [ ! -z "$DEBUG" ]
then
  echo curl -s -H "Origin: ${ORIGIN}" -H "Authorization: Bearer ${JWT_TOKEN}" -XDELETE "${API_URL}/v1/affiliation/${project}/delete_identity/${id}"
fi

curl -s -H "Origin: ${ORIGIN}" -H "Authorization: Bearer ${JWT_TOKEN}" -XDELETE "${API_URL}/v1/affiliation/${project}/delete_identity/${id}"
