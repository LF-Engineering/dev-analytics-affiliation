#!/bin/bash
# N_CPUS - set number of CPUS, N_CPUS=1 enables singlethreaded mode
# DA_AFF_API_SQL_OUT=1 - output SQL queries
# USE_SEARCH_IN_MERGE - special flag to use search patter in merge queries performed after the main search
if [ -z "${LOG_LEVEL}" ]
then
  LOG_LEVEL=info
fi
if [ -z "$API_DB_ENDPOINT" ]
then
  export API_DB_ENDPOINT='host=127.0.0.1 user=postgres password=postgrespwd dbname=dev_analytics port=15432 sslmode=disable'
fi
if [ -z "$SH_DB_ENDPOINT" ]
then
  export SH_DB_ENDPOINT='sortinghat:pwd@tcp(localhost:13306)/sortinghat?charset=utf8'
fi
if [ -z "$SH_DB_RO_ENDPOINT" ]
then
  export SH_DB_RO_ENDPOINT='ro_user:pwd@tcp(localhost:13306)/sortinghat?charset=utf8'
fi
if [ -z "$SYNC_URL" ]
then
  export SYNC_URL="`cat helm/da-affiliation/secrets/SYNC_URL.prod.secret`"
fi
if [ -z "$AUTH0_AUDIENCE" ]
then
  export AUTH0_AUDIENCE=`cat helm/da-affiliation/secrets/AUTH0_AUDIENCE.prod.secret`
fi
if [ -z "$ELASTIC_URL" ]
then
  export ELASTIC_URL='http://127.0.0.1:19200'
fi
export ELASTIC_USERNAME=''
export ELASTIC_PASSWORD=''
if [ -z "$CORS_ALLOWED_ORIGINS" ]
then
  export CORS_ALLOWED_ORIGINS='https://insights.test.platform.linuxfoundation.org, https://lfanalytics.io, http://127.0.0.1'
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
