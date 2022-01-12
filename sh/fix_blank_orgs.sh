#!/bin/bash
# ES_URL=...
# INDEX=sds-cncf-k8s-github-issue
# API_URL=prod
# PSLUG='cncf/k8s'
if [ -z "${ES_URL}" ]
then
  echo "$0: you need to specify ES_URL=..."
  exit 1
fi
if [ -z "${INDEX}" ]
then
  echo "$0: you need to specify INDEX=..."
  exit 2
fi
if [ -z "${API_URL}" ]
then
  echo "$0: you need to specify API_URL=..."
  exit 3
fi
if [ -z "${PSLUG}" ]
then
  echo "$0: you need to specify PSLUG=..."
  exit 4
fi
updates=0
fails=0
uuids=$(curl -s -XPOST -H 'Content-type: application/json' "${ES_URL}/${INDEX}/_search?size=10000" -d'{"query":{"term":{"author_org_name":""}}}' | jq -rS '.hits.hits[]._source.author_uuid' | sort | uniq)
for uuid in $uuids
do
  echo "UUID: $uuid"
  data=$(curl -s -XPOST -H 'Content-type: application/json' "${ES_URL}/${INDEX}/_search?size=10000" -d"{\"query\":{\"bool\":{\"must\":[{\"term\":{\"author_org_name\":\"\"}},{\"term\":{\"author_uuid\":\"${uuid}\"}}]}}}" | jq --compact-output -rS '.hits.hits')
  ids=$(echo $data | jq -rS '.[]._id')
  dates=($(echo $data | jq -rS '.[]._source.metadata__updated_on'))
  i=0
  for id in $ids
  do
    date=${dates[$i]}
    echo "uuid: $uuid, date: $date, doc_id=$id, i=$i"
    i=$((i+1))
    res=$(JWT_TOKEN=`cat secret/lgryglicki.${API_URL}.token` ./sh/curl_get_affiliation_both.sh "$PSLUG" $uuid $date | jq '.')
    org=$(echo $res | jq -r '.org')
    if ( [ "$org" = "null" ] || [ "$org" = "" ] )
    then
      echo "Cannot get org for uuid: $uuid, date: $date: $res"
      fails=$((fails+1))
    else
      echo "uuid: $uuid, date: $date --> $org"
      res=$(curl -s -XPOST -H 'Content-type: application/json' "${ES_URL}/${INDEX}/_update_by_query?conflicts=proceed" -d"{\"query\":{\"term\":{\"_id\":\"$id\"}},\"script\":\"ctx._source.author_org_name='${org}'\"}" | jq -rS '.')
      upd=$(echo $res | jq -r '.updated')
      if ( [ "$upd" = "null" ] || [ "$upd" = "" ] || [ "$upd" = "0" ] )
      then
        echo "Failed updating uuid: $uuid, date: $date, doc_id=$id: $res"
      else
        echo "uuid: $uuid, date: $date, doc_id=$id updated org to: $org"
        updates=$((updates+1))
      fi
    fi
  done
done
echo "Done $updates updates"
if [ ! "$fails" = "0" ]
then
  echo "$fails failed"
fi
