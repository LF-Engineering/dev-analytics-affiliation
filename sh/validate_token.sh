#!/bin/bash
if [ -z "${1}" ]
then
  echo "$0: you need to specify env: test|prod"
  exit 1
fi
if [ -z "${2}" ]
then
  fn="secret/lgryglicki.${1}.token"
else
  fn="${2}"
fi
token="`cat ${fn}`"
if [ -z "${token}" ]
then
  echo "$0: cannot read file ${fn}"
  exit 2
fi
audience="`cat helm/da-affiliation/secrets/AUTH0_AUDIENCE.${1}.secret`"
if [ -z "$audience" ]
then
  echo "$0: cannot read file helm/da-affiliation/secrets/AUTH0_AUDIENCE.${1}.secret"
  exit 3
fi
if [ ! -z "$DEBUG" ]
then
  echo "curl -s -XGET -H 'Content-Type: application/json' -H 'Authorization: Bearer ${token}' '${audience}authping'"
  curl -s -XGET -H 'Content-Type: application/json' -H "Authorization: Bearer ${token}" "${audience}authping"
fi
result=`curl -s -XGET -H 'Content-Type: application/json' -H "Authorization: Bearer ${token}" "${audience}authping"`
msg=`echo "${result}" | jq '.Message'`
if [ ! "${msg}" = "null" ]
then
  echo "Token invalid: ${msg}"
else
  sub=`echo "${result}" | jq '.sub'`
  echo "Valid: ${sub}"
fi
