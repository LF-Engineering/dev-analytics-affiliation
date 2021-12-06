#!/bin/bash
# Example run: ESURL="https://[redacted]" CONDITION="author_name in ('ryanpetersonOF', 'pjbroadbent', 'Li Cui', 'Michael M. Coates', 'Harsimran Singh', 'Luis Espinola', 'Michael Coates', 'Daniel Kocielinski', 'nisse', 'brybailey', 'David H', 'deadbeef', 'brandtr', 'James Leftley', 'kjellander', 'Nicholas Goodman', 'David Hamberlin', 'Sergio Garcia Murillo', 'magjed', 'Sami Kalliomäki', 'malaysf', 'Aziz Yokubjonov', 'Danil Chapovalov', 'Mark Josling', 'Aziem Chawdhary', 'michaelt', 'Aaron Griswold')" ./sh/es_documents.sh
if [ -z "$ESURL" ]
then
  echo "$0: you need to specify ESURL=..."
  exit 1
fi
if [ -z "$CONDITION" ]
then
  query="{\"query\":\"select origin, project, project_slug, author_uuid, author_id, author_name, author_org_name as org, min(metadata__updated_on) as dt_from, max(metadata__updated_on) as dt_to, count(*) as cnt from \\\"IDXNAME\\\" group by origin, project, project_slug, author_uuid, author_id, author_name, author_org_name\",\"fetch_size\":10000}"
  querymin="{\"query\":\"select origin, author_uuid, author_id, author_name, author_org_name as org, min(metadata__updated_on) as dt_from, max(metadata__updated_on) as dt_to, count(*) as cnt from \\\"IDXNAME\\\" group by origin, author_uuid, author_id, author_name, author_org_name\",\"fetch_size\":10000}"
else
  query="{\"query\":\"select origin, project, project_slug, author_uuid, author_id, author_name, author_org_name as org, min(metadata__updated_on) as dt_from, max(metadata__updated_on) as dt_to, count(*) as cnt from \\\"IDXNAME\\\" where ${CONDITION} group by origin, project, project_slug, author_uuid, author_id, author_name, author_org_name\",\"fetch_size\":10000}"
  querymin="{\"query\":\"select origin, author_uuid, author_id, author_name, author_org_name as org, min(metadata__updated_on) as dt_from, max(metadata__updated_on) as dt_to, count(*) as cnt from \\\"IDXNAME\\\" where ${CONDITION} group by origin, author_uuid, author_id, author_name, author_org_name\",\"fetch_size\":10000}"
fi
echo $query > documents-query.json.secret
echo $querymin > documents-querymin.json.secret
> .log.secret
#for idx in $(curl -s "${ESURL}/_cat/indices?format=json" | jq -rS '.[].index' | grep -E "^(bitergia.+|sds-.*)" | grep -Ev '(-repository(-for-merge)?|-raw|-googlegroups|-slack|-dockerhub|-last-action-date-cache|-social_media|finosmeetings)$' | grep -Ev '\-onion_')
for idx in $(curl -s "${ESURL}/_cat/indices?format=json" | jq -rS '.[].index' | grep -E "^(bitergia.+|sds-.*)" | grep -Ev '(-repository(-for-merge)?|-raw|-last-action-date-cache|-social_media)$' | grep -Ev '\-onion_')
do
  data=`cat documents-query.json.secret`
  data=${data/IDXNAME/$idx}
  echo $data > q.json.secret
  res=$(curl -s -XPOST -H 'Content-Type: application/json' "${ESURL}/_sql?format=json" -d@q.json.secret | jq -r '.rows')
  if [ ! "$res" = "[]" ]
  then
    if [ "$res" = "null" ]
    then
      data=`cat documents-querymin.json.secret`
      data=${data/IDXNAME/$idx}
      echo $data > q.json.secret
      res=$(curl -s -XPOST -H 'Content-Type: application/json' "${ESURL}/_sql?format=json" -d@q.json.secret | jq -r '.rows')
      if ( [ ! "$res" = "[]" ] && [ ! "$res" = "null" ] )
      then
        echo "special $idx: $res" | tee -a documents.log.secret
      fi
    else
      echo "$idx: $res" | tee -a documents.log.secret
    fi
  fi
done
