#!/bin/bash
if [ -z "${ES_URL}" ]
then
  ES_URL="`cat ./helm/da-affiliation/secrets/ELASTIC_URL.prod.secret`"
fi
if [ -z "${1}" ]
then
  echo "$0: please specify project slug like 'lfn-onap' or 'lfn'"
  exit 1
fi
if [ -z "${FROM}" ]
then
  FROM=0
fi
if [ -z "${TO}" ]
then
  TO=2552790984700
fi
if [ -z "${SIZE}" ]
then
  SIZE=10
fi
fn=/tmp/top_contributors.json
function on_exit {
  rm -f "${fn}"
}
cp sh/top_contributors.json /tmp/
trap on_exit EXIT
vim --not-a-term -c "%s/param_from/${FROM}/g" -c "%s/param_to/${TO}/g" -c "%s/param_size/${SIZE}/g" -c 'wq!' "$fn"
curl -s -H "Content-Type: application/json" "${ES_URL}/sds-${1}-*,-*-raw,-*-for-merge/_search" -d "@${fn}" | jq
