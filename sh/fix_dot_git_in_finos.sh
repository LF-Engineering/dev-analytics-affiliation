#!/bin/bash
for field in origin repo_name tag
do
  echo "cleanup $field"
  values=$(curl -s -XPOST -H 'Content-Type: application/json' "${ES}/_sql?format=csv" -d"{\"query\":\"select $field,1 from \\\"sds-finos-*,-*-raw\\\" where $field like '%.git' group by $field\",\"fetch_size\":10000}")
  for data in $values
  do
    arr=(${data//,/ })
    value=${arr[0]}
    if [ "$value" = "$field" ]
    then
      continue
    fi
    new_value=${value::-4}
    echo "$value -> $new_value"
    curl -s -XPOST -H 'Content-type: application/json' "${ES}/sds-finos-*,-*-raw/_update_by_query?conflicts=proceed" -d"{\"query\":{\"term\":{\"${field}\":\"${value}\"}},\"script\":\"ctx._source.${field}='${new_value}'\"}" | jq -rS '.'
  done
done
