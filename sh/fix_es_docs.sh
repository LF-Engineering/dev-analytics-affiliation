#!/bin/bash
# Example run: INDICES='idx1 idx2 ... idxN' UUIDS='uuid1 uuid2 ... uuidN' ORG='OrgName' ESURL='https://...' ./sh/fix_es_docs.sh 
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
  q="{\"script\":\"ctx._source.author_org_name='${ORG}'\",\"query\":{\"terms\":{\"author_uuid\":["
  for uuid in $UUIDS
  do
    q="${q}\"${uuid}\","
  done
  q="${q::-1}]}}}"
  echo $q > q.json.secret
  echo -n "${idx}: "
  curl -s -XPOST -H 'Content-Type: application/json' "${ESURL}/${idx}/_update_by_query?conflicts=proceed" -d@q.json.secret | jq -rS .
done
