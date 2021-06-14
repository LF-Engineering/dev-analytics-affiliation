#!/bin/bash
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
if [ "$2" = "" ]
then
  echo "$0: you need to specify UUID as a 2nd arg"
  exit 3
fi
uuid=$2
cmd="$shacc \"select 1 from profiles where uuid = '${uuid}'\""
res=$(eval "${cmd}")
if [ -z "$res" ]
then
  echo "$0: UUID $uuid cannot be found"
  exit 4
fi
if [ "$3" = "" ]
then
  echo "$0: you need to specify at least one affiliation like 'Company Name;YYYY-MM-DD;YYYY-MM-DD'"
  exit 5
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
  from="${from} 00:00:00"
  to="${to} 00:00:00"
  cmd="$shacc \"select count(id) from organizations where name = '${company}'\""
  res=$(eval "${cmd}")
  if [ ! "$res" = "1" ]
  then
    echo "$0: company ${company} maps to ${res} results, there should be exactly 1 match, aborting"
    exit 6
  fi
  cmd="$shacc \"select id from organizations where name = '${company}' limit 1\""
  cid=$(eval "${cmd}")
  echo "$cid: ${arr[0]}: $from - $to"
done

#delete from enrollments where uuid = 'cbdf896ace2068132f2dc0423254065c5d031bbd';
#insert into enrollments(start, end, uuid, organization_id, project_slug, role) select '1900-01-01 00:00:00', '2006-01-01 00:00:00', 'cbdf896ace2068132f2dc0423254065c5d031bbd', id, 'finos-f', 'Contributor' from organizations where name = 'Individual - No Account';
#insert into enrollments(start, end, uuid, organization_id, project_slug, role) select '2006-01-01 00:00:00', '2100-01-01 00:00:00', 'cbdf896ace2068132f2dc0423254065c5d031bbd', id, 'finos-f', 'Contributor' from organizations where name = 'Scott Logic Ltd';
#select e.project_slug, o.name, e.start, e.end from enrollments e, organizations o where e.organization_id = o.id and e.uuid = 'cbdf896ace2068132f2dc0423254065c5d031bbd' order by e.project_slug, e.start;
