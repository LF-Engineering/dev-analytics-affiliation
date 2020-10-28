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
domain="`cat helm/da-affiliation/secrets/AUTH0_DOMAIN.${1}.secret`"
if [ -z "$domain" ]
then
  echo "$0: cannot read file helm/da-affiliation/secrets/AUTH0_DOMAIN.${1}.secret"
  exit 2
fi
audience="`cat helm/da-affiliation/secrets/AUTH0_AUDIENCE.${1}.secret`"
if [ -z "$audience" ]
then
  echo "$0: cannot read file helm/da-affiliation/secrets/AUTH0_AUDIENCE.${1}.secret"
  exit 3
fi
clientid="`cat helm/da-affiliation/secrets/AUTH0_CLIENT_ID.${1}.secret`"
if [ -z "$clientid" ]
then
  echo "$0: cannot read file helm/da-affiliation/secrets/AUTH0_CLIENT_ID.${1}.secret"
  exit 4
fi
clientsecret="`cat helm/da-affiliation/secrets/AUTH0_CLIENT_SECRET.${1}.secret`"
if [ -z "$clientsecret" ]
then
  echo "$0: cannot read file helm/da-affiliation/secrets/AUTH0_CLIENT_SECRET.${1}.secret"
  exit 5
fi
payload="{\"grant_type\":\"client_credentials\",\"client_id\":\"${clientid}\",\"client_secret\":\"${clientsecret}\",\"audience\":\"${audience}\",\"scope\":\"access:api\"}"
if [ ! -z "$DEBUG" ]
then
  echo "curl -XPOST -H 'Content-Type: application/json' ${domain}/oauth/token -d'${payload}'"
  #curl -s -XPOST -H 'Content-Type: application/json' "${domain}/oauth/token" -d"${payload}"
fi
token=`curl -s -XPOST -H 'Content-Type: application/json' "${domain}/oauth/token" -d"${payload}" | jq -r '.access_token'`
echo "${token}" > "$fn"
