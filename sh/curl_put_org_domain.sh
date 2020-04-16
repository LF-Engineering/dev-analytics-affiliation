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

ov="false"
if [ "$4" = "1" ]
then
  ov="true"
fi

top="false"
if [ "$5" = "1" ]
then
  top="true"
fi

skip_enrollments="false"
if [ "$6" = "1" ]
then
  skip_enrollments="true"
fi

if [ ! -z "$DEBUG" ]
then
  echo "$project $org $dom $ov $top $skip_enrollments"
  echo curl -s -H "Origin: ${ORIGIN}" -H "Authorization: Bearer ${JWT_TOKEN}" -XPUT "${API_URL}/v1/affiliation/${project}/add_domain/${org}/${dom}?overwrite=${ov}&is_top_domain=${top}&skip_enrollments=${skip_enrollments}"
fi

curl -s -H "Origin: ${ORIGIN}" -H "Authorization: Bearer ${JWT_TOKEN}" -XPUT "${API_URL}/v1/affiliation/${project}/add_domain/${org}/${dom}?overwrite=${ov}&is_top_domain=${top}&skip_enrollments=${skip_enrollments}"
