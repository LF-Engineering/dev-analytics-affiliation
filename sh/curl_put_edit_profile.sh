#!/bin/bash
. ./sh/shared.sh
if [ -z "$2" ]
then
  echo "$0: please specify profile UUID as a 2nd arg"
  exit 2
fi
uuid=$(rawurlencode "${2}")
extra=''

for prop in name email gender gender_acc is_bot country_code
do
  if [ ! -z "${!prop}" ]
  then
    encoded=$(rawurlencode "${!prop}")
    if [ -z "$extra" ]
    then
      extra="?$prop=${encoded}"
    else
      extra="${extra}&$prop=${encoded}"
    fi
  fi
done

if [ ! -z "$DEBUG" ]
then
  echo curl -s -H "Origin: ${ORIGIN}" -H "Authorization: Bearer ${JWT_TOKEN}" -XPUT "${API_URL}/v1/affiliation/${project}/edit_profile/${uuid}${extra}"
fi

curl -s -H "Origin: ${ORIGIN}" -H "Authorization: Bearer ${JWT_TOKEN}" -XPUT "${API_URL}/v1/affiliation/${project}/edit_profile/${uuid}${extra}"
