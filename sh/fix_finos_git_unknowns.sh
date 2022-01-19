#!/bin/bash
# ES_URL=...
# API_URL=prod
export INDEX='sds-finos-*-git,-*-raw'
if [ -z "${ES_URL}" ]
then
  echo "$0: you need to specify ES_URL=..."
  exit 1
fi
if [ -z "${API_URL}" ]
then
  echo "$0: you need to specify API_URL=..."
  exit 2
fi
gupdates=0
gunknowns=0
gfails=0
pslugs=$(curl -s -XPOST -H 'Content-Type: application/json' "${ES_URL}/_sql?format=json" -d"{\"query\":\"select project_slug from \\\"${INDEX}\\\" group by project_slug\",\"fetch_size\":10000}" | jq -rS '.rows[][0]')
for pslug in $pslugs
do
  updates=0
  unknowns=0
  fails=0
  # echo "Project: $pslug"
  uuids=$(curl -s -XPOST -H 'Content-type: application/json' "${ES_URL}/${INDEX}/_search?size=10000" -d"{\"query\":{\"bool\":{\"must\":[{\"term\":{\"author_org_name\":\"Unknown\"}},{\"term\":{\"project_slug\":\"${pslug}\"}}]}}}" | jq -rS '.hits.hits[]._source.author_uuid' | sort | uniq)
  for uuid in $uuids
  do
    # echo "UUID: $uuid"
    data=$(curl -s -XPOST -H 'Content-type: application/json' "${ES_URL}/${INDEX}/_search?size=10000" -d"{\"query\":{\"bool\":{\"must\":[{\"term\":{\"author_org_name\":\"Unknown\"}},{\"term\":{\"project_slug\":\"${pslug}\"}},{\"term\":{\"author_uuid\":\"${uuid}\"}}]}}}" | jq --compact-output -rS '.hits.hits')
    ids=$(echo $data | jq -rS '.[]._id')
    dates=($(echo $data | jq -rS '.[]._source.metadata__updated_on'))
    i=0
    for id in $ids
    do
      date=${dates[$i]}
      echo "uuid: $uuid, date: $date, doc_id=$id, i=$i"
      i=$((i+1))
      res=$(JWT_TOKEN=`cat secret/lgryglicki.${API_URL}.token` ./sh/curl_get_affiliation_single.sh "$pslug" $uuid $date | jq '.')
      org=$(echo $res | jq -r '.org')
      if ( [ "$org" = "null" ] || [ "$org" = "" ] )
      then
        echo "Cannot get org for uuid: $uuid, date: $date: $res"
        fails=$((fails+1))
      elif [ "$org" = "Unknown" ]
      then
        echo "Still Unknown for uuid: $uuid, date: $date"
        unknowns=$((unknowns+1))
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
  if [ ! "$updates" = "0" ]
  then
    echo "$pslug: done $updates updates"
    gupdates=$((gupdates+updates))
  fi
  if [ ! "$unknowns" = "0" ]
  then
    echo "$pslug: $unknowns still unknown"
    gunknowns=$((gunknowns+unknowns))
  fi
  if [ ! "$fails" = "0" ]
  then
    echo "$pslug: $fails failed"
    gfails=$((gfails+fails))
  fi
done
if [ ! "$gupdates" = "0" ]
then
  echo "done $gupdates updates"
fi
if [ ! "$gunknowns" = "0" ]
then
  echo "$gunknowns still unknown"
fi
if [ ! "$gfails" = "0" ]
then
  echo "$gfails failed"
fi
