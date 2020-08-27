#!/bin/bash
if [ -z "$MYSQL" ]
then
  echo "Please specify full mysql connect command, something like MYSQL='mysql -hdburl -uuser -ppassword dbname'"
  exit 1
fi
if [ -z "${JSON}" ]
then
  JSON="${HOME}/dev/go/src/github.com/cncf/devstats/github_users.json"
fi
MYSQL="${MYSQL} -NBAe "
for name in "$@"
do
  echo "${name}:"
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
  for uuid in ${uuids}
  do
    echo "#${i} uuid: ${uuid}"
    echo "Identities:"
    cmd="$MYSQL \"select source, username, regexp_replace(email, '([^\\s@]+)@([^\\s@]+)', '\\\\\\\\1\\!\\\\\\\\2') from identities where uuid = '${uuid}' order by source\""
    data=$(eval "${cmd}")
    if [ -z "${data}" ]
    then
      echo "Identity ${uuid} not found"
      ((i=i+1))
      echo
      continue
    fi
    ary=(${data})
    j="0"
    while true
    do
      src="${ary[${j}]}"
      login="${ary[((j+1))]}"
      email="${ary[((j+2))]}"
      if [ -z "${src}" ]
      then
        break
      fi
      echo -e "${src}\t${login}\t${email}"
      if [ ! "${login}" = "NULL" ]
      then
        logins["${login}"]="1"
      fi
      if [ ! "${email}" = "NULL" ]
      then
        emails["${email}"]="1"
      fi
      ((j=j+3))
    done
    echo "Enrollments:"
    cmd="$MYSQL \"select e.project_slug, date(e.start), date(e.end), o.name, e.role from enrollments e, organizations o where e.organization_id = o.id and e.uuid = '${uuid}' order by e.project_slug, e.start\""
    data=$(eval "${cmd}")
    if [ -z "${data}" ]
    then
      echo "No enrollments for ${uuid}"
    fi
    echo "${data}"
    ((i=i+1))
    echo
  done
  i="1"
  conds=""
  for login in "${!logins[@]}"
  do
    cond=".login==\"${login}\""
    if [ -z "${conds}" ]
    then
      conds="${cond}"
    else
      conds="${conds} or ${cond}"
    fi
  done
  for email in "${!emails[@]}"
  do
    cond=".email==\"${email}\""
    if [ -z "${conds}" ]
    then
      conds="${cond}"
    else
      conds="${conds} or ${cond}"
    fi
  done
  echo "Cond: ${conds}"
  js=`jq -r ".[] | select(${conds}) | .login + \"/\" + .email + \": \" + (.affiliation // \"-\")" "${JSON}"`
  echo 
done
