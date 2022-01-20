#!/bin/bash
declare -A glprojects
lines=$(cat projects.json.secret | jq -rS '. | to_entries | .[].value.meta.title')
while read prj
do
  prj=${prj//$'\n'/}
  prj=${prj//$'\r'/}
  if [ "$prj" = "null" ]
  then
    continue
  fi
  # echo "GiLab project: '$prj'"
  glprojects[$prj]=1
done <<< "$lines"
lines=$(cat sh/finos_prjs.secret)
while read prj
do
  prj=${prj//$'\n'/}
  prj=${prj//$'\r'/}
  found="${glprojects[$prj]}"
  if [ -z "$found" ]
  then
    echo "Project '$prj' not found in GitLab"
  fi
done <<< "$lines"
