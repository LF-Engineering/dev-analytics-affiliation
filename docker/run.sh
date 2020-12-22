#!/bin/bash
if [ -z "${DOCKER_USER}" ]
then
  echo "$0: you need to set docker user via DOCKER_USER=username"
  exit 1
fi
if [ -z "${LOG_LEVEL}" ]
then
  LOG_LEVEL=info
fi
SYNC_URL="`cat helm/da-affiliation/secrets/SYNC_URL.prod.secret`"
API_DB_ENDPOINT='host=172.17.0.1 user=postgres password=postgrespwd dbname=dev_analytics port=15432 sslmode=disable'
SH_DB_ENDPOINT='sortinghat:pwd@tcp(172.17.0.1:13306)/sortinghat?charset=utf8'
SH_DB_RO_ENDPOINT='sortinghat:pwd@tcp(172.17.0.1:13306)/sortinghat?charset=utf8'
AUTH0_DOMAIN=`cat secret/auth0.domain`
AUTH0_CLIENT_ID=`cat secret/auth0.client_id`
AUTH0_USERNAME_CLAIM=`cat secret/auth0.username_claim`
ELASTIC_URL='http://172.17.0.1:19200'
ELASTIC_USERNAME=''
ELASTIC_PASSWORD=''
docker run -p 18080:8080 -e "USE_SEARCH_IN_MERGE=${USE_SEARCH_IN_MERGE}" -e "N_CPUS=${N_CPUS}" -e "LOG_LEVEL=${LOG_LEVEL}" -e "SYNC_URL=${SYNC_URL}" -e "API_DB_ENDPOINT=${API_DB_ENDPOINT}" -e "SH_DB_ENDPOINT=${SH_DB_ENDPOINT}" -e "SH_DB_RO_ENDPOINT=${SH_DB_RO_ENDPOINT}" -e "AUTH0_DOMAIN=${AUTH0_DOMAIN}" -e "AUTH0_CLIENT_ID=${AUTH0_CLIENT_ID}" -e "AUTH0_USERNAME_CLAIM=${AUTH0_USERNAME_CLAIM}" -e "ELASTIC_URL=${ELASTIC_URL}" -e "ELASTIC_USERNAME=${ELASTIC_USERNAME}" -e "ELASTIC_PASSWORD=${ELASTIC_PASSWORD}" -it "${DOCKER_USER}/dev-analytics-affiliation-api" "/usr/bin/main"
