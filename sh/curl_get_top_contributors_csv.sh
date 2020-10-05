#!/bin/bash
export SKIP_TOKEN=1
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
dataSource=''
if [ ! -z "$9" ]
then
  dataSource=$(rawurlencode "${9}")
fi

if [ -z "${JWT_TOKEN}" ]
then
  if [ ! -z "$DEBUG" ]
  then
    echo curl -s -i -H "Origin: ${ORIGIN}" -H 'Content-Type: application/octet-stream' -XGET "${API_URL}/v1/affiliation/${project}/top_contributors_csv?from=${from}&to=${to}&limit=${limit}&offset=${offset}&search=${search}&sort_field=${sortField}&sort_order=${sortOrder}&data_source=${dataSource}"
    curl -s -i -H "Origin: ${ORIGIN}" -H 'Content-Type: application/octet-streams' -XGET "${API_URL}/v1/affiliation/${project}/top_contributors_csv?from=${from}&to=${to}&limit=${limit}&offset=${offset}&search=${search}&sort_field=${sortField}&sort_order=${sortOrder}&data_source=${dataSource}"
  else
    curl -s -H "Origin: ${ORIGIN}" -H 'Content-Type: application/octet-streams' -XGET "${API_URL}/v1/affiliation/${project}/top_contributors_csv?from=${from}&to=${to}&limit=${limit}&offset=${offset}&search=${search}&sort_field=${sortField}&sort_order=${sortOrder}&data_source=${dataSource}"
  fi
else
  if [ ! -z "$DEBUG" ]
  then
    echo curl -s -i -H "Origin: ${ORIGIN}" -H 'Content-Type: application/octet-stream' -H "Authorization: Bearer ${JWT_TOKEN}" -XGET "${API_URL}/v1/affiliation/${project}/top_contributors_csv?from=${from}&to=${to}&limit=${limit}&offset=${offset}&search=${search}&sort_field=${sortField}&sort_order=${sortOrder}&data_source=${dataSource}"
    curl -s -i -H "Origin: ${ORIGIN}" -H 'Content-Type: application/octet-streams' -H "Authorization: Bearer ${JWT_TOKEN}" -XGET "${API_URL}/v1/affiliation/${project}/top_contributors_csv?from=${from}&to=${to}&limit=${limit}&offset=${offset}&search=${search}&sort_field=${sortField}&sort_order=${sortOrder}&data_source=${dataSource}"
  else
    curl -s -H "Origin: ${ORIGIN}" -H 'Content-Type: application/octet-streams' -H "Authorization: Bearer ${JWT_TOKEN}" -XGET "${API_URL}/v1/affiliation/${project}/top_contributors_csv?from=${from}&to=${to}&limit=${limit}&offset=${offset}&search=${search}&sort_field=${sortField}&sort_order=${sortOrder}&data_source=${dataSource}"
  fi
fi
