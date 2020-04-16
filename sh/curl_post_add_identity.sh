#!/bin/bash
. ./sh/shared.sh
if [ -z "$2" ]
then
  echo "$0: please specify identity source as a 2nd arg"
  exit 2
fi
ssource=$(rawurlencode "${2}")
extra=''

for prop in name email username uuid
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
  echo curl -i -s -H "Origin: ${ORIGIN}" -H "Authorization: Bearer ${JWT_TOKEN}" -XPOST "${API_URL}/v1/affiliation/${project}/add_identity/${ssource}${extra}"
  curl -i -s -H "Origin: ${ORIGIN}" -H "Authorization: Bearer ${JWT_TOKEN}" -XPOST "${API_URL}/v1/affiliation/${project}/add_identity/${ssource}${extra}"
else
  curl -s -H "Origin: ${ORIGIN}" -H "Authorization: Bearer ${JWT_TOKEN}" -XPOST "${API_URL}/v1/affiliation/${project}/add_identity/${ssource}${extra}"
fi
