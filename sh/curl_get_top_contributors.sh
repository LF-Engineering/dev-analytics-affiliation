#!/bin/bash
. ./sh/shared.sh
from=''
if [ ! -z "$2" ]
then
  from=$(rawurlencode "${2}")
fi
to=''
if [ ! -z "$3" ]
then
  to=$(rawurlencode "${3}")
fi
limit=10
if [ ! -z "$4" ]
then
  limit=$(rawurlencode "${4}")
fi
offset=0
if [ ! -z "$5" ]
then
  offset=$(rawurlencode "${5}")
fi
search=''
if [ ! -z "$6" ]
then
  search=$(rawurlencode "${6}")
fi
sortField=''
if [ ! -z "$7" ]
then
  sortField=$(rawurlencode "${7}")
fi
sortOrder=''
if [ ! -z "$8" ]
then
  sortOrder=$(rawurlencode "${8}")
fi

if [ ! -z "$DEBUG" ]
then
  echo curl -i -s -H "Origin: ${ORIGIN}" -H 'Content-Type: application/json' -H "Authorization: Bearer ${JWT_TOKEN}" -XGET "${API_URL}/v1/affiliation/${project}/top_contributors?from=${from}&to=${to}&limit=${limit}&offset=${offset}&search=${search}&sort_field=${sortField}&sort_order=${sortOrder}"
  curl -i -s -H "Origin: ${ORIGIN}" -H 'Content-Type: application/json' -H "Authorization: Bearer ${JWT_TOKEN}" -XGET "${API_URL}/v1/affiliation/${project}/top_contributors?from=${from}&to=${to}&limit=${limit}&offset=${offset}&search=${search}&sort_field=${sortField}&sort_order=${sortOrder}"
else
  curl -s -H "Origin: ${ORIGIN}" -H 'Content-Type: application/json' -H "Authorization: Bearer ${JWT_TOKEN}" -XGET "${API_URL}/v1/affiliation/${project}/top_contributors?from=${from}&to=${to}&limit=${limit}&offset=${offset}&search=${search}&sort_field=${sortField}&sort_order=${sortOrder}"
fi
