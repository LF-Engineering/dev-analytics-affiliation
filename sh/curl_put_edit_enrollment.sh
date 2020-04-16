#!/bin/bash
. ./sh/shared.sh
if [ -z "$2" ]
then
  echo "$0: please specify profile uuid as a 2nd arg"
  exit 2
fi
if [ -z "$3" ]
then
    echo "$0: please specify organization name (must exist) as a 3rd arg"
  exit 3
fi
uuid=$(rawurlencode "${2}")
orgName=$(rawurlencode "${3}")
extra=''

for prop in start end new_start new_end merge
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
  echo curl -s -H "Origin: ${ORIGIN}" -H "Authorization: Bearer ${JWT_TOKEN}" -XPUT "${API_URL}/v1/affiliation/${project}/edit_enrollment/${uuid}/${orgName}${extra}"
fi

curl -s -H "Origin: ${ORIGIN}" -H "Authorization: Bearer ${JWT_TOKEN}" -XPUT "${API_URL}/v1/affiliation/${project}/edit_enrollment/${uuid}/${orgName}${extra}"
