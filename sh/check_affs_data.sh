#!/bin/bash
# Example:
# PATTERN='sds-lfn-opnfv-*,-*-raw,-*-for-merge' MYSQL='mysql -hhost -uuser -ppass db' ./sh/check_login.sh ES='https://elastic:user@url.us-west-1.aws.found.io:port' 'lukasz Gryglicki'
if [ -z "$MYSQL" ]
then
  echo "Please specify full mysql connect command, something like MYSQL='mysql -hdburl -uuser -ppassword dbname'"
  exit 1
fi
if [ -z "$ES" ]
then
  echo "Please specify full ElasticSearch URL, something like ES=elastic.host.com"
  exit 2
fi
if [ -z "${PATTERN}" ]
then
  PATTERN='sds-*-git*,-*-raw,-*-for-merge'
fi
MYSQL="${MYSQL} -NBAe "
for name in "$@"
do
  echo "${name}:"
  echo "SortingHat:"
  cmd="$MYSQL \"select uuid from profiles where name = '${name}' union select uuid from identities where name = '${name}'\""
  uuids=$(eval "${cmd}")
  if [ -z "${uuids}" ]
  then
    echo "No profiles/identities found for ${name}"
    continue
  fi
  i="1"
  declare -A logins=()
  declare -A emails=()
  declare -A eemails=()
  for uuid in ${uuids}
  do
    echo "#${i} uuid: ${uuid}"
    echo "Profiles:"
    cmd="$MYSQL \"select if(email='','NULL',email), regexp_replace(if(email='','NULL',email), '([^\\s@]+)@([^\\s@]+)', '\\\\\\\\1\\!\\\\\\\\2'), regexp_replace(if(name='','NULL',name), '\\\\\\\\s', '---') from profiles where uuid = '${uuid}' order by email\""
    data=$(eval "${cmd}")
    if [ -z "${data}" ]
    then
      echo "Profile ${uuid} not found"
    else
      ary=(${data})
      j="0"
      while true
      do
        email="${ary[${j}]}"
        eemail="${ary[((j+1))]}"
        pname="${ary[((j+2))]//---/ }"
        if [ -z "${email}" ]
        then
          break
        fi
        echo -e "${email}\t${pname}"
        if [ ! "${email}" = "NULL" ]
        then
          emails["${email}"]="1"
          eemails["${eemail}"]="1"
        fi
        ((j=j+3))
      done
    fi
    echo "Identities:"
    cmd="$MYSQL \"select if(source='','NULL',source), if(username='','NULL',username), if(email='','NULL',email), regexp_replace(if(email='','NULL',email), '([^\\s@]+)@([^\\s@]+)', '\\\\\\\\1\\!\\\\\\\\2'), regexp_replace(if(name='','NULL',name), '\\\\\\\\s', '---') from identities where uuid = '${uuid}' order by source\""
    data=$(eval "${cmd}")
    if [ -z "${data}" ]
    then
      echo "Identity ${uuid} not found"
    else
      ary=(${data})
      j="0"
      while true
      do
        src="${ary[${j}]}"
        login="${ary[((j+1))]}"
        email="${ary[((j+2))]}"
        eemail="${ary[((j+3))]}"
        iname="${ary[((j+4))]//---/ }"
        if [ -z "${src}" ]
        then
          break
        fi
        echo -e "${src}\t${login}\t${email}\t${iname}"
        if ( [ "${src}" = "github" ] && [ ! "${login}" = "NULL" ] )
        then
          logins["${login}"]="1"
        fi
        if [ ! "${email}" = "NULL" ]
        then
          emails["${email}"]="1"
          eemails["${eemail}"]="1"
        fi
        ((j=j+5))
      done
    fi
    echo "Enrollments:"
    cmd="$MYSQL \"select e.project_slug, date(e.start), date(e.end), o.name, e.role from enrollments e, organizations o where e.organization_id = o.id and e.uuid = '${uuid}' order by e.project_slug, e.start\""
    data=$(eval "${cmd}")
    if [ -z "${data}" ]
    then
      echo "No enrollments for ${uuid}"
    else
      echo "${data}"
    fi
    echo "ElasticSearch ${PATTERN}: author_uuid/author_id: ${uuid}"
    #es=`curl -s -XPOST -H 'Content-type: application/json' "${ES}/_sql?format=tsv" -d"{\"query\":\"select metadata__gelk_backend_name, origin, author_id, author_uuid, author_org_name, count(*) as cnt, min(grimoire_creation_date), max(grimoire_creation_date) from \\\\\"${PATTERN}\\\\\" where author_uuid in ('${uuid}') or author_id in ('${uuid}') group by metadata__gelk_backend_name, author_id, author_uuid, origin, author_org_name order by cnt desc\"}" | tail -n +2`
    #echo curl -s -XPOST -H 'Content-type: application/json' "${ES}/_sql?format=tsv" -d"{\"query\":\"select metadata__gelk_backend_name, origin, author_id, author_uuid, author_org_name, count(*) as cnt, min(grimoire_creation_date), max(grimoire_creation_date) from \\\\\"${PATTERN}\\\\\" where author_uuid in ('${uuid}') or author_id in ('${uuid}') group by metadata__gelk_backend_name, author_id, author_uuid, origin, author_org_name order by cnt desc\"}"
    es=`curl -s -XPOST -H 'Content-type: application/json' "${ES}/_sql?format=tsv" -d"{\"query\":\"select metadata__gelk_backend_name, origin, author_id, author_uuid, author_org_name, count(*) as cnt, min(grimoire_creation_date), max(grimoire_creation_date) from \\\\\"${PATTERN}\\\\\" where author_uuid in ('${uuid}') or author_id in ('${uuid}') group by metadata__gelk_backend_name, author_id, author_uuid, origin, author_org_name order by cnt desc\"}"`
    echo "${es}"
    ((i=i+1))
  done
  i="1"
  conds=""
  lcond=""
  econd=""
  for login in "${!logins[@]}"
  do
    cond=".login==\"${login}\""
    if [ -z "${conds}" ]
    then
      conds="${cond}"
    else
      conds="${conds} or ${cond}"
    fi
    if [ -z "${lcond}" ]
    then
      lcond="'${login}'"
    else
      lcond="${lcond},'${login}'"
    fi
  done
  for email in "${!emails[@]}"
  do
    if [ -z "${econd}" ]
    then
      econd="'${email}'"
    else
      econd="${econd},'${email}'"
    fi
  done
  for eemail in "${!eemails[@]}"
  do
    cond=".email==\"${eemail}\""
    if [ -z "${conds}" ]
    then
      conds="${cond}"
    else
      conds="${conds} or ${cond}"
    fi
  done
  echo "ElasticSearch ${PATTERN}: author_name: ${name}"
  #echo curl -s -XPOST -H 'Content-type: application/json' "${ES}/_sql?format=tsv" -d"{\"query\":\"select metadata__gelk_backend_name, origin, author_id, author_uuid, author_org_name, count(*) as cnt, min(grimoire_creation_date), max(grimoire_creation_date) from \\\\\"${PATTERN}\\\\\" where author_name in ('${name}') group by metadata__gelk_backend_name, origin, author_id, author_uuid, author_org_name order by cnt desc\"}"
  es=`curl -s -XPOST -H 'Content-type: application/json' "${ES}/_sql?format=tsv" -d"{\"query\":\"select metadata__gelk_backend_name, origin, author_id, author_uuid, author_org_name, count(*) as cnt, min(grimoire_creation_date), max(grimoire_creation_date) from \\\\\"${PATTERN}\\\\\" where author_name in ('${name}') group by metadata__gelk_backend_name, origin, author_id, author_uuid, author_org_name order by cnt desc\"}"`
  echo "${es}"
done
