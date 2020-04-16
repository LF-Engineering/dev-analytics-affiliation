#!/bin/bash
# DA_AFF_API_SQL_OUT=1 - output SQL queries
if [ -z "$API_DB_ENDPOINT" ]
then
  export API_DB_ENDPOINT='host=127.0.0.1 user=postgres password=postgrespwd dbname=dev_analytics port=15432 sslmode=disable'
fi
if [ -z "$SH_DB_ENDPOINT" ]
then
  export SH_DB_ENDPOINT='sortinghat:pwd@tcp(localhost:13306)/sortinghat?charset=utf8'
fi
if [ -z "$AUTH0_DOMAIN" ]
then
  export AUTH0_DOMAIN=`cat secret/auth0.domain`
fi
if [ -z "$AUTH0_CLIENT_ID" ]
then
  export AUTH0_CLIENT_ID=`cat secret/auth0.client_id`
fi
if [ -z "$AUTH0_USERNAME_CLAIM" ]
then
  export AUTH0_USERNAME_CLAIM=`cat secret/auth0.username_claim`
fi
if [ -z "$ELASTIC_URL" ]
then
  export ELASTIC_URL='http://127.0.0.1:19200'
fi
export ELASTIC_USERNAME=''
export ELASTIC_PASSWORD=''
if [ -z "$CORS_ALLOWED_ORIGINS" ]
then
  export CORS_ALLOWED_ORIGINS='https://test.lfanalytics.io, https://lfanalytics.io, http://127.0.0.1'
fi
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
