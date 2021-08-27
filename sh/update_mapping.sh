#!/bin/bash
# args: env index-name mapping.json
if [ -z "$1" ]
then
  echo "$0: you need to specify env: test|prod"
  exit 1
fi
ESURL="`cat ../sync-data-sources/helm-charts/sds-helm/sds-helm/secrets/ES_URL.${1}.secret`"
if [ -z "$ESURL" ]
then
  echo "$0: cannot get US URL value"
  exit 2
fi
if [ -z "$2" ]
then
  echo "$0: you need to provide the index name as a 2nd arg"
  exit 3
fi
if [ -z "$3" ]
then
  echo "$0: you need to provide mapping file as a 3rd arg"
  exit 4
fi
index="${2}"
mapping=$(cat "${3}" | jq --compact-output -rS '.')
if [ -z "$mapping" ]
then
  echo "$0: cannot read mapping from '$3'"
  exit 6
fi
suff=`tr -dc a-z0-9 </dev/urandom | head -c 13 ; echo ''`
tmpindex="${index}-${suff}"
curl -s -XDELETE "${ESURL}/${tmpindex}" 1>/dev/null 2>/dev/null
com="curl -s -XPUT -H 'Content-Type: application/json' \"${ESURL}/${tmpindex}\" -d'${mapping}' | jq -rS '.'"
#echo $com
eval $com || exit 7
curl -s -XPOST -H 'Content-Type: application/json' "${ESURL}/_reindex?refresh=true&wait_for_completion=true" -d"{\"conflicts\":\"proceed\",\"source\":{\"index\":\"${index}\"},\"dest\":{\"index\":\"tmpindex\"}}" | jq -rS '.' || exit 8
curl -s -XDELETE "${ESURL}/${index}" | jq -rS '.' || exit 9
com="curl -s -XPUT -H 'Content-Type: application/json' \"${ESURL}/${index}\" -d'${mapping}' | jq -rS '.'"
#echo $com
eval $com || exit 9
curl -s -XPOST -H 'Content-Type: application/json' "${ESURL}/_reindex?refresh=true&wait_for_completion=true" -d"{\"conflicts\":\"proceed\",\"source\":{\"index\":\"${tmpindex}\"},\"dest\":{\"index\":\"index\"}}" | jq -rS '.' || exit 10
curl -s -XDELETE "${ESURL}/${tmpindex}" | jq -rS '.'
echo "All OK"
