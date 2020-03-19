#!/bin/bash
if [ -z "$1" ]
then
  echo "$0: please provide project slug as a first arg"
  exit 1
fi
if [ -z "$ES_URL" ]
then
  export ES_URL="http://127.0.0.1:19200"
fi
idx="sds-${1//\//-}"
if [ -z "$RAW" ]
then
  curl -H 'Content-Type: application/json' "${ES_URL}/${idx}-*,-${idx}-*-raw/_search" -d'{"size":10,"aggs":{"unaffiliated":{"terms":{"field":"author_org_name","missing":"null","size":10}}}}' 2>/dev/null | jq
  curl -H 'Content-Type: application/json' "${ES_URL}/${idx}-*,-${idx}-*-raw/_search" -d'{"size":10, "aggs":{"unaffiliated":{"filter":{"terms":{"author_org_name":["Unknown","NotFound","","-","?"]}},"aggs":{"unaffiliated":{"terms":{"field":"author_uuid","missing":"","size": 10}}}}}}' 2>/dev/null | jq
else
  curl -H 'Content-Type: application/json' "${ES_URL}/${idx}-*,-${idx}-*-raw/_search" -d'{"size":0, "aggs":{"unaffiliated":{"filter":{"terms":{"author_org_name":["Unknown","NotFound","","-","?"]}},"aggs":{"unaffiliated":{"terms":{"field":"author_uuid","missing":"","size":10000}}}}}}' 2>/dev/devll
fi
