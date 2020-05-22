#!/bin/bash
if [ -z "${1}" ]
then
  echo "$0: you need to specify env: dev|test|prod"
  exit 1
fi
if [ -z "${2}" ]
then
  if [ "${1}" = "prod" ]
  then
    fn="secret/lgryglicki.token"
  else
    fn="secret/lgryglicki.${1}.token"
  fi
else
  fn="${2}"
fi
url="`cat secret/AUTH0_URL.${1}.secret`"
if [ -z "$url" ]
then
  echo "$0: cannot file secret/AUTH0_URL.${1}.secret file"
  exit 2
fi
audience="`cat secret/AUTH0_AUDIENCE.${1}.secret`"
if [ -z "$audience" ]
then
  echo "$0: cannot file secret/AUTH0_AUDIENCE.${1}.secret file"
  exit 3
fi
clientid="`cat secret/AUTH0_CLIENT_ID.${1}.secret`"
if [ -z "$clientid" ]
then
  echo "$0: cannot file secret/AUTH0_CLIENT_ID.${1}.secret file"
  exit 4
fi
clientsecret="`cat secret/AUTH0_CLIENT_SECRET.${1}.secret`"
if [ -z "$clientsecret" ]
then
  echo "$0: cannot file secret/AUTH0_CLIENT_SECRET.${1}.secret file"
  exit 5
fi
payload="{\"grant_type\":\"client_credentials\",\"client_id\":\"${clientid}\",\"client_secret\":\"${clientsecret}\",\"audience\":\"${audience}\",\"scope\":\"access:api\"}"
#curl -s -XPOST -H 'Content-Type: application/json' "${url}/oauth/token" -d"${payload}"
token=`curl -s -XPOST -H 'Content-Type: application/json' "${url}/oauth/token" -d"${payload}" | jq -r '.access_token'`
echo "${token}" > "$fn"
