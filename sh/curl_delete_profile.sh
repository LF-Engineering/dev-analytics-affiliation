#!/bin/bash
. ./sh/shared.sh
if [ -z "$2" ]
then
  echo "$0: please specify profile uuid as a 2nd arg"
  exit 2
fi
uuid=$(rawurlencode "${2}")
archive="false"
if [ "$3" = "1" ]
then
  archive="true"
fi

if [ ! -z "$DEBUG" ]
then
  echo curl -i -s -H "Origin: ${ORIGIN}" -H "Authorization: Bearer ${JWT_TOKEN}" -XDELETE "${API_URL}/v1/affiliation/${project}/delete_profile/${uuid}?archive=${archive}"
  curl -i -s -H "Origin: ${ORIGIN}" -H "Authorization: Bearer ${JWT_TOKEN}" -XDELETE "${API_URL}/v1/affiliation/${project}/delete_profile/${uuid}?archive=${archive}"
else
  curl -s -H "Origin: ${ORIGIN}" -H "Authorization: Bearer ${JWT_TOKEN}" -XDELETE "${API_URL}/v1/affiliation/${project}/delete_profile/${uuid}?archive=${archive}"
fi
