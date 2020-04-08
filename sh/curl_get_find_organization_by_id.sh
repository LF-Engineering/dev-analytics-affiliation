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
  echo "$0: please specify organization ID as a 2nd arg"
  exit 2
fi
if [ -z "$API_URL" ]
then
  export API_URL="http://127.0.0.1:8080"
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
orgID=$(rawurlencode "${2}")

if [ ! -z "$DEBUG" ]
then
  echo curl -s -H "Authorization: Bearer ${JWT_TOKEN}" -XGET "${API_URL}/v1/affiliation/${project}/find_organization_by_id/${orgID}"
fi

curl -s -H "Authorization: Bearer ${JWT_TOKEN}" -XGET "${API_URL}/v1/affiliation/${project}/find_organization_by_id/${orgID}"
