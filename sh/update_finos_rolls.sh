#!/bin/bash
# CHECK=1 - only check data
# Example use: 
# [CHECK=1] ./sh/update_finos_rolls.sh prod 7d25f805bcc1886acd05a50efdacd1c95dff8912 'Individual - No Account;;2007-09-01' 'Sourcesense;2007-09-01;2012-01-31' 'Individual - No Account;2012-01-31;2012-04-01' 'Alfresco;2012-04-01;2016-04-30' 'Finos;2016-04-30;2020-04-08' 'The Linux Foundation;2020-04-08;'
if [ "$1" = "" ]
then
  echo "$0: you need to specify env as a 1st arg: prod|test"
  exit 1
fi
env=$1
if [ ! -f "helm/da-affiliation/secrets/SH_DB_CMDLINE.$env.secret" ]
then
  echo "$0: missing helm/da-affiliation/secrets/SH_DB_CMDLINE.$env.secret file"
  exit 2
fi
shacc="`cat helm/da-affiliation/secrets/SH_DB_CMDLINE.$env.secret`"
shacc="${shacc} -NBAe "
if [ ! -f "helm/da-affiliation/secrets/ELASTIC_URL.$env.secret" ]
then
  echo "$0: helm/da-affiliation/secrets/ELASTIC_URL.$env.secret file"
  exit 3
fi
esacc="`cat helm/da-affiliation/secrets/ELASTIC_URL.$env.secret`"
if [ "$2" = "" ]
then
  echo "$0: you need to specify UUID as a 2nd arg"
  exit 4
fi
uuid=$2
cmd="$shacc \"select 1 from profiles where uuid = '${uuid}'\""
res=$(eval "${cmd}")
if [ -z "$res" ]
then
  echo "$0: UUID $uuid cannot be found"
  exit 5
fi
if [ ! -z "$CHECK" ]
then
  echo "SH enrollments:"
  cmd="$shacc \"select e.project_slug, o.name, e.start, e.end from enrollments e, organizations o where e.organization_id = o.id and e.uuid = '${uuid}' order by e.project_slug, e.start\""
  eval "${cmd}"
  echo "ES data:"
  cmd="curl -s -XPOST -H 'Content-Type: application/json' \"${esacc}/_sql?format=txt\" -d\"{\\\"query\\\":\\\"select origin, author_uuid, author_org_name, count(*) as cnt, min(metadata__updated_on) as first, max(metadata__updated_on) as last from \\\\\\\"sds-finos-*,-*-raw,-*-temp\\\\\\\" where author_uuid = '${uuid}' and author_org_name = 'Unknown' group by origin, author_uuid, author_org_name limit 10000\\\"}\""
  eval "$cmd"
  cmd="curl -s -XPOST -H 'Content-Type: application/json' \"${esacc}/_sql?format=txt\" -d\"{\\\"query\\\":\\\"select author_uuid, author_org_name, count(*) as cnt, min(metadata__updated_on) as first, max(metadata__updated_on) as last from \\\\\\\"sds-finos-*,-*-raw,-*-temp\\\\\\\" where author_uuid = '${uuid}' group by author_uuid, author_org_name limit 10000\\\"}\""
  eval "$cmd"
  exit 0
fi
if [ "$3" = "" ]
then
  echo "$0: you need to specify at least one affiliation like 'Company Name;YYYY-MM-DD;YYYY-MM-DD'"
  exit 6
fi
for ((pass = 0; pass <= 1; pass++ ))
do
  if [ "$pass" = "1" ]
  then
    cmd="$shacc \"delete from enrollments where uuid = '${uuid}'\""
    eval "${cmd}"
    sts=$?
    if [ ! "$sts" = "0" ]
    then
      echo "drop status $sts"
      exit 7
    fi
    echo "dropped enrollments for ${uuid}"
  fi
  for ((i = 3; i <= $#; i++ ))
  do
    data="${!i}"
    IFS=';'
    arr=($data)
    unset IFS
    company="${arr[0]}"
    from="${arr[1]}"
    to="${arr[2]}"
    if [ -z "$from" ]
    then
      from="1900-01-01"
    fi
    if [ -z "$to" ]
    then
      to="2100-01-01"
    fi
    efrom="${from}T00:00:00Z"
    eto="${to}T00:00:00Z"
    from="${from} 00:00:00"
    to="${to} 00:00:00"
    if [ "$pass" = "0" ]
    then
      cmd="$shacc \"select count(id) from organizations where name = '${company}'\""
      res=$(eval "${cmd}")
      if [ ! "$res" = "1" ]
      then
        echo "$0: company ${company} maps to ${res} results, there should be exactly 1 match, aborting"
        exit 8
      fi
    fi
    cmd="$shacc \"select id from organizations where name = '${company}' limit 1\""
    cid=$(eval "${cmd}")
    if [ "$pass" = "1" ]
    then
      cmd="$shacc \"insert into enrollments(start, end, uuid, organization_id, project_slug, role) select '${from}', '${to}', '${uuid}', ${cid}, 'finos-f', 'Contributor'\""
      eval "${cmd}"
      sts=$?
      if [ ! "$sts" = "0" ]
      then
        echo "insert status $sts"
      fi
      echo "inserted ${uuid} enrollment"
      cmd="curl -s -XPOST -H 'Content-Type: application/json' '${esacc}/sds-finos-*,-*-raw,-*-temp/_update_by_query?wait_for_completion=true&conflicts=proceed&refresh=true' -d\"{\\\"script\\\":{\\\"inline\\\":\\\"ctx._source.author_org_name='${company}';\\\"},\\\"query\\\":{\\\"bool\\\":{\\\"must\\\":[{\\\"term\\\":{\\\"author_uuid\\\":\\\"${uuid}\\\"}},{\\\"range\\\":{\\\"metadata__updated_on\\\":{\\\"gte\\\":\\\"${efrom}\\\",\\\"lt\\\":\\\"${eto}\\\"}}}]}}}\""
      eval "${cmd}"
      sts=$?
      echo ''
      if [ ! "$sts" = "0" ]
      then
        echo "$0: failed to update ES, status: $sts"
      fi
      cmd="curl -s -XPOST -H 'Content-Type: application/json' \"${esacc}/_sql?format=txt\" -d\"{\\\"query\\\":\\\"select author_org_name, count(*) as cnt, min(metadata__updated_on) as first, max(metadata__updated_on) as last from \\\\\\\"sds-finos-*,-*-raw,-*-temp\\\\\\\" where author_uuid = '${uuid}' and metadata__updated_on >= '${efrom}' and metadata__updated_on < '${eto}'  group by author_uuid, author_org_name limit 10000\\\"}\""
      echo "$cid: ${arr[0]}: $efrom - $eto"
      eval "$cmd"
    else
      echo "$cid: ${arr[0]}: $from - $to"
    fi
  done
  if [ "$pass" = "1" ]
  then
    echo "Final SH enrollments:"
    cmd="$shacc \"select e.project_slug, o.name, e.start, e.end from enrollments e, organizations o where e.organization_id = o.id and e.uuid = '${uuid}' order by e.project_slug, e.start\""
    eval "${cmd}"
    echo "Final ES data:"
    cmd="curl -s -XPOST -H 'Content-Type: application/json' \"${esacc}/_sql?format=txt\" -d\"{\\\"query\\\":\\\"select author_uuid, author_org_name, count(*) as cnt, min(metadata__updated_on) as first, max(metadata__updated_on) as last from \\\\\\\"sds-finos-*,-*-raw,-*-temp\\\\\\\" where author_uuid = '${uuid}' group by author_uuid, author_org_name limit 10000\\\"}\""
    eval "$cmd"
  fi
done
