#!/bin/bash
# curl -s -XPOST -H 'Content-type: application/json' "${ES_URL}/_sql?format=csv "-d"{\"query\":\"select author_uuid, count(*) as cnt from \\\"sds-cncf-*,*-raw,*-for-merge\\\" where author_org_name = 'Unknown' and not (author_bot = true) and author_uuid is not null and author_uuid != '' group by author_uuid having cnt > 20 order by cnt desc limit 10000\"}"
if [ -z "$MYSQL" ]
then
  echo "Please specify full mysql connect command, something like MYSQL='mysql -hdburl -uuser -ppassword dbname' $*"
  exit 1
fi
MYSQL="${MYSQL} -NBAe "
for f in `cat ~/unknowns_es.txt`
do
  ary=(${f//,/ })
  uuid=${ary[0]}
  cnt=${ary[1]}
  cmd="$MYSQL \"select uuid from profiles where uuid = '${uuid}'\""
  uuid2=$(eval "${cmd}")
  if [ -z "$uuid2" ]
  then
    echo "cannot find $uuid profile"
    continue
  fi
  if [ ! "$uuid" = "$uuid2" ]
  then
    echo "should not happen $uuid != $uuid2"
    continue
  fi
  cmd="$MYSQL \"select source, username, email from identities where uuid = '${uuid}' and source like 'git%'\""
  data=$(eval "${cmd}")
  cmd="$MYSQL \"select * from enrollments where uuid = '${uuid}' and project_slug like 'cncf/%'\""
  rols=$(eval "${cmd}")
  if ( [ -z "${rols}" ] && [ ! -z "${data}" ] )
  then
    echo "For $uuid having $cnt docs we have identity data but no rols:"
    echo "${data}"
  fi
done
