#!/bin/bash
# Example:
# PSQL='sudo -u postgres psql db' MYSQL='mysql -hhost -uuser -ppass db' ./sh/check_login.sh 'lukasz Gryglicki'
if [ -z "$MYSQL" ]
then
  echo "Please specify full mysql connect command, something like MYSQL='mysql -hdburl -uuser -ppassword dbname'"
  exit 1
fi
if [ -z "$PSQL" ]
then
  echo "Please specify full postgresql connect command, something like PSQL='sudo -u postgres psql dbname'"
  exit 2
fi
if [ -z "${JSON}" ]
then
  JSON="${HOME}/dev/go/src/github.com/cncf/devstats/github_users.json"
fi
MYSQL="${MYSQL} -NBAe "
PSQL="${PSQL} -F$'\t' -tAc "
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
    echo "Identities:"
    cmd="$MYSQL \"select source, username, email, regexp_replace(email, '([^\\s@]+)@([^\\s@]+)', '\\\\\\\\1\\!\\\\\\\\2') from identities where uuid = '${uuid}' order by source\""
    data=$(eval "${cmd}")
    if [ -z "${data}" ]
    then
      echo "Identity ${uuid} not found"
      ((i=i+1))
      continue
    fi
    ary=(${data})
    j="0"
    while true
    do
      src="${ary[${j}]}"
      login="${ary[((j+1))]}"
      email="${ary[((j+2))]}"
      eemail="${ary[((j+3))]}"
      if [ -z "${src}" ]
      then
        break
      fi
      echo -e "${src}\t${login}\t${email}"
      if ( [ "${src}" = "github" ] && [ ! "${login}" = "NULL" ] )
      then
        logins["${login}"]="1"
      fi
      if [ ! "${email}" = "NULL" ]
      then
        emails["${email}"]="1"
        eemails["${eemail}"]="1"
      fi
      ((j=j+4))
    done
    echo "Enrollments:"
    cmd="$MYSQL \"select e.project_slug, date(e.start), date(e.end), o.name, e.role from enrollments e, organizations o where e.organization_id = o.id and e.uuid = '${uuid}' order by e.project_slug, e.start\""
    data=$(eval "${cmd}")
    if [ -z "${data}" ]
    then
      echo "No enrollments for ${uuid}"
    else
      echo "${data}"
    fi
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
  echo "CNCF JSON:"
  js=`jq -r ".[] | select(${conds}) | .login + \"/\" + .email + \": \" + (.affiliation // \"-\")" "${JSON}"`
  if [ -z "${js}" ]
  then
    echo "Nothing found for: ${conds}"
  else
    echo "${js}"
  fi
  pcond=''
  if [ ! -z "${lcond}" ]
  then
    pcond="select id from gha_actors where login in (${lcond})"
  fi
  if [ ! -z "${econd}" ]
  then
    if [ -z "${pcond}" ]
    then
      pcond="select actor_id from gha_actors_emails where email in (${econd})"
    else
      pcond="${pcond} union select actor_id from gha_actors_emails where email in (${econd})"
    fi
  fi
  echo "DevStats DB:"
  if [ -z "${pcond}" ]
  then
    echo "No email or login to search data found"
    continue
  fi
  echo 'Affiliations:'
  cmd="$PSQL \"select date(dt_from), date(dt_to), company_name, source from gha_actors_affiliations where actor_id in (${pcond})\""
  data=$(eval "${cmd}")
  if [ -z "${data}" ]
  then
    echo "No affiliations found for ${pcond}"
  else
    echo "${data}"
  fi
  echo 'Commits:'
  cmd="$PSQL \"select count(distinct sha) as cnt, date(min(dup_created_at)), date(max(dup_created_at)) from gha_commits where committer_id in (${pcond}) or author_id in (${pcond}) order by cnt desc\""
  data=$(eval "${cmd}")
  echo "${data}"
  contrib="'IssuesEvent', 'PullRequestEvent', 'PushEvent', 'CommitCommentEvent', 'IssueCommentEvent', 'PullRequestReviewCommentEvent'"
  echo 'Contributions:'
  cmd="$PSQL \"select count(*) as cnt, date(min(created_at)), date(max(created_at)) from gha_events where type in (${contrib}) and actor_id in (${pcond})\""
  data=$(eval "${cmd}")
  echo "${data}"
  echo 'Contribution types:'
  cmd="$PSQL \"select type, count(*) as cnt, date(min(created_at)), date(max(created_at)) from gha_events where type in (${contrib}) and actor_id in (${pcond}) group by type order by cnt desc\""
  data=$(eval "${cmd}")
  echo "${data}"
done
