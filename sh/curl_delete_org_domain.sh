#!/bin/bash
if [ -z "$JWT_TOKEN" ]
then
  echo "$0: please sepcify your JWT toen via JWT_TOKEN=..."
  exit 1
fi
if [ -z "$1" ]
then
  echo "$0: please specify organization name as a 1st arg"
  exit 2
fi
if [ -z "$2" ]
then
  echo "$0: please specify domain as a 2nd arg"
  exit 3
fi
if [ -z "$3" ]
then
  echo "$0: please specify project slug as a 3rd arg"
  exit 4
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

org=$(rawurlencode "${1}")
dom=$(rawurlencode "${2}")
scope=$(rawurlencode "${3}")

if [ ! -z "$DEBUG" ]
then
  echo "$org $dom $scope $ov $top"
  echo curl -s -H "Authorization: Bearer ${JWT_TOKEN}" -XDELETE "${API_URL}/v1/affiliation/${scope}/remove_domain/${org}/${dom}"
fi

curl -s -H "Authorization: Bearer ${JWT_TOKEN}" -XDELETE "${API_URL}/v1/affiliation/${scope}/remove_domain/${org}/${dom}"
