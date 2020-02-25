#!/bin/bash
if [ -z "$ONLYRUN" ]
then
  make swagger && make build && API_DB_ENDPOINT='host=127.0.0.1 user=postgres password=postgrespwd dbname=dev_analytics port=15432 sslmode=disable' SH_DB_ENDPOINT='sortinghat:pwd@tcp(localhost:13306)/sortinghat?charset=utf8' AUTH0_DOMAIN=`cat auth0.domain` AUTH0_CLIENT_ID=`cat auth0.client_id` AUTH0_USERNAME_CLAIM=`cat auth0.username_claim` make run
else
  API_DB_ENDPOINT='host=127.0.0.1 user=postgres password=postgrespwd dbname=dev_analytics port=15432 sslmode=disable' SH_DB_ENDPOINT='sortinghat:pwd@tcp(localhost:13306)/sortinghat?charset=utf8' AUTH0_DOMAIN=`cat auth0.domain` AUTH0_CLIENT_ID=`cat auth0.client_id` AUTH0_USERNAME_CLAIM=`cat auth0.username_claim` make run
fi
