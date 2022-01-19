#!/bin/bash
export INDEX='sds-finos-*-git,-*-raw'
uuids=$(curl -s -XPOST -H 'Content-Type: application/json' "${ES}/_sql?format=csv" -d"{\"query\":\"select author_id, author_uuid, count(*) as cnt, 1 from \\\"${INDEX}\\\" where author_org_name = 'Unknown' group by author_id, author_uuid order by cnt desc\",\"fetch_size\":10000}")
for data in $uuids
do
  arr=(${data//,/ })
  id=${arr[0]}
  uuid=${arr[1]}
  cnt=${arr[2]}
  if [ "$id" = "author_id" ]
  then
    continue
  fi
  echo "$id $uuid $cnt"
  cmd="${SH} \"select uuid from identities where id = '$id'\""
  uuid2=$(eval "${cmd}")
  if [ ! "$uuid" = "$uuid2" ]
  then
    echo "$id uuid mismatch $uuid != $uuid2"
    # continue
    res=$(curl -s -XPOST -H 'Content-type: application/json' "${ES}/${INDEX}/_update_by_query?conflicts=proceed" -d"{\"query\":{\"term\":{\"author_id\":\"${id}\"}},\"script\":\"ctx._source.author_uuid='${uuid2}'\"}" | jq -rS '.')
    upd=$(echo $res | jq -r '.updated')
    if ( [ "$upd" = "null" ] || [ "$upd" = "" ] || [ "$upd" = "0" ] )
    then
      echo "failed updating author_id $id uuid $uuid -> $uuid2,$res"
    else
      echo "updated author_id: $id uuid $uuid -> $uuid2"
    fi
  fi
  cmd="${SH} \"select name from identities where id = '$id'\""
  name=$(eval "${cmd}")
  echo "$id $uuid $cnt -> '$name'"
  cmd="${SH} \"select email from identities where id = '$id'\""
  email=$(eval "${cmd}")
  echo "$id $uuid $cnt '$name' -> $email"
  # cmd="${SH} \"select e.uuid, count(*) as cnt from identities i, enrollments e where i.uuid = e.uuid and i.name is not null and trim(i.name) != '' and i.name = '$name' and i.id != '$id' and i.email != '$email' group by e.uuid order by cnt desc limit 1\""
  cmd="${SH} \"select e.uuid, count(*) as cnt from identities i, enrollments e where i.uuid = e.uuid and i.name is not null and trim(i.name) != '' and i.name = '$name' and i.id != '$id' and i.source = 'git' group by e.uuid order by cnt desc limit 1\""
  res=$(eval "${cmd}")
  new_uuid=(${res[0]})
  echo "$id $uuid/$uuid2 $cnt '$name' $email -> $new_uuid"
  if ( [ ! -z "$new_uuid" ] && [ ! "$uuid2" = "$new_uuid" ] )
  then
    echo "need to merge $uuid2 into $new_uuid"
    API_URL=prod JWT_TOKEN=`cat secret/lgryglicki.prod.token` ./sh/curl_put_merge_unique_identities.sh 'finos-f' "$uuid2" "$new_uuid"
  fi
done
