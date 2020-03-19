#!/bin/bash
export API_DB_ENDPOINT='host=127.0.0.1 user=postgres password=postgrespwd dbname=dev_analytics port=15432 sslmode=disable'
export SH_DB_ENDPOINT='sortinghat:pwd@tcp(localhost:13306)/sortinghat?charset=utf8'
export AUTH0_DOMAIN=`cat secret/auth0.domain`
export AUTH0_CLIENT_ID=`cat secret/auth0.client_id`
export AUTH0_USERNAME_CLAIM=`cat secret/auth0.username_claim`
if [ -z "$ELASTIC_URL" ]
then
  export ELASTIC_URL='http://127.0.0.1:19200'
fi
export ELASTIC_USERNAME=''
export ELASTIC_PASSWORD=''
if [ -z "$ONLYRUN" ]
then
  make swagger && make build && make run
else
  if [ -z "$NOCHECKS" ]
  then
    make run
  else
    make fastrun
  fi
fi
