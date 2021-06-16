#!/bin/bash
if [ -z "$1" ]
then
  echo "$0: you need to specify env as a 1st arg: test|prod"
  exit 1
fi
if [ -z "$2" ]
then
  echo "$0: you need to specify project as a 2nd arg"
  exit 2
fi
if [ ! -f "secret/lgryglicki.$1.token" ]
then
  echo "$0: missing secret/lgryglicki.$1.token file, use ./sh/get_token.sh $1 to get it"
  exit 3
fi
token="`cat secret/lgryglicki.$1.token`"
curl -s -H "Authorization: Bearer ${token}" "https://api-gw.platform.linuxfoundation.org/project-service/v1/projects/${2}" | jq '.'
