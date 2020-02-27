#!/bin/bash
if [ -z "$JWT_TOKEN" ]
then
  echo "$0: please sepcify your JWT toen via JWT_TOKEN=..."
  exit 1
fi
if [ -z "$1" ]
then
  echo "$0: please specify project slug as a 1st arg"
  exit 2
fi
if [ -z "$2" ]
then
  echo "$0: please specify from uidentity uuid as a 2nd arg"
  exit 3
fi
if [ -z "$3" ]
then
  echo "$0: please specify to uidentity uuid as a 3rd arg"
  exit 4
fi

rawurlencode() {
  local string="${1}"
  local strlen=${#string}
  local encoded=""
  local pos c o
  for (( pos=0 ; pos<strlen ; pos++ )); do
     c=${string:$pos:1}
     case "$c" in
        [-_.~a-zA-Z0-9] ) o="${c}" ;;
        * )               printf -v o '%%%02x' "'$c"
     esac
     encoded+="${o}"
  done
  echo "${encoded}"
  REPLY="${encoded}"
}

project=$(rawurlencode "${1}")
from_uuid=$(rawurlencode "${2}")
to_uuid=$(rawurlencode "${3}")

if [ ! -z "$DEBUG" ]
then
  echo "$project $from_uuid $to_uuid"
  echo curl -H 'Accept: application/json' -H "Authorization: Bearer ${JWT_TOKEN}" -XPUT "http://127.0.0.1:8080/v1/affiliation/${project}/merge_profiles/${from_uuid}/${to_uuid}"
fi
curl -H 'Accept: application/json' -H "Authorization: Bearer ${JWT_TOKEN}" -XPUT "http://127.0.0.1:8080/v1/affiliation/${project}/merge_profiles/${from_uuid}/${to_uuid}"
