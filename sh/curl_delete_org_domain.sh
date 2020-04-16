#!/bin/bash
. ./sh/shared.sh
if [ -z "$2" ]
then
  echo "$0: please specify organization name as a 2nd arg"
  exit 3
fi
if [ -z "$3" ]
then
  echo "$0: please specify domain as a 3rd arg"
  exit 4
fi
org=$(rawurlencode "${2}")
dom=$(rawurlencode "${3}")

if [ ! -z "$DEBUG" ]
then
  echo "$project $org $dom $ov $top"
  echo curl -i -s -H "Origin: ${ORIGIN}" -H "Authorization: Bearer ${JWT_TOKEN}" -XDELETE "${API_URL}/v1/affiliation/${project}/remove_domain/${org}/${dom}"
  curl -i -s -H "Origin: ${ORIGIN}" -H "Authorization: Bearer ${JWT_TOKEN}" -XDELETE "${API_URL}/v1/affiliation/${project}/remove_domain/${org}/${dom}"
else
  curl -s -H "Origin: ${ORIGIN}" -H "Authorization: Bearer ${JWT_TOKEN}" -XDELETE "${API_URL}/v1/affiliation/${project}/remove_domain/${org}/${dom}"
fi
