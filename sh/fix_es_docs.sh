#!/bin/bash
# Example run: INDICES='idx1 idx2 ... idxN' UUIDS='uuid1 uuid2 ... uuidN' ORG='OrgName' [FROM=2011-01-01 TO=2012-07-01] [DBG=1] ESURL='https://...' ./sh/fix_es_docs.sh 
if [ -z "$ESURL" ]
then
  echo "$0: you need to specify ESURL=..."
  exit 1
fi
if [ -z "$INDICES" ]
then
  echo "$0: you need to specify INDICES='idx1 idx2 ... idxN'"
  exit 2
fi
if [ -z "$UUIDS" ]
then
  echo "$0: you need to specify UUIDS='uuid1 uuid2 ... uuidN'"
  exit 3
fi
if [ -z "$ORG" ]
then
  echo "$0: you need to specify ORG='Org Name'"
  exit 4
fi
for idx in $INDICES
do
  q="{\"script\":\"ctx._source.author_org_name='${ORG}'\","
  if [ -z "${FROM}" ]
  then
    q="${q}\"query\":{\"terms\":{\"author_uuid\":["
    for uuid in $UUIDS
    do
      q="${q}\"${uuid}\","
    done
    q="${q::-1}]}}}"
  else
    q="${q}\"query\":{\"bool\":{\"must\":[{\"terms\":{\"author_uuid\":["
    for uuid in $UUIDS
    do
      q="${q}\"${uuid}\","
    done
    q="${q::-1}]}},{\"range\":{\"metadata__updated_on\":{\"gte\":\"${FROM}\",\"lt\":\"${TO}\"}}}]}}}"
  fi
  echo $q > q.json.secret
  if [ ! -z "${DBG}" ]
  then
    cat q.json.secret
    cat q.json.secret | jq -rS .
  fi
  echo -n "${idx}: "
  curl -s -XPOST -H 'Content-Type: application/json' "${ESURL}/${idx}/_update_by_query?conflicts=proceed" -d@q.json.secret | jq -rS .
done
