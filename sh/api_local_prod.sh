#!/bin/bash
export LOG_LEVEL=debug
export STAGE=prod
export ONLYRUN=1
export NOCHECKS=1
export AUTH0_DOMAIN="`cat helm/da-affiliation/secrets/AUTH0_DOMAIN.prod.secret`"
export ELASTIC_URL="`cat helm/da-affiliation/secrets/ELASTIC_URL.prod.secret`"
export SH_DB_ENDPOINT="`cat helm/da-affiliation/secrets/SH_DB_ENDPOINT.prod.secret`"
export SH_DB_RO_ENDPOINT="`cat helm/da-affiliation/secrets/SH_DB_ENDPOINT.prod.secret`"
export API_DB_ENDPOINT="`cat helm/da-affiliation/secrets/API_DB_ENDPOINT.prod.secret`"
export PLATFORM_USER_SERVICE_ENDPOINT="`cat helm/da-affiliation/secrets/PLATFORM_USER_SERVICE_ENDPOINT.prod.secret`"
export PLATFORM_ORG_SERVICE_ENDPOINT="`cat helm/da-affiliation/secrets/PLATFORM_ORG_SERVICE_ENDPOINT.prod.secret`"
export ELASTIC_CACHE_URL="`cat helm/da-affiliation/secrets/ELASTIC_CACHE_URL.prod.secret`"
export ELASTIC_CACHE_USERNAME="`cat helm/da-affiliation/secrets/ELASTIC_CACHE_USERNAME.prod.secret`"
export ELASTIC_CACHE_PASSWORD="`cat helm/da-affiliation/secrets/ELASTIC_CACHE_PASSWORD.prod.secret`"
export ELASTIC_LOG_URL="`cat helm/da-affiliation/secrets/ELASTIC_LOG_URL.prod.secret`"
export ELASTIC_LOG_USERNAME="`cat helm/da-affiliation/secrets/ELASTIC_LOG_USERNAME.prod.secret`"
export ELASTIC_LOG_PASSWORD="`cat helm/da-affiliation/secrets/ELASTIC_LOG_PASSWORD.prod.secret`"
export AUTH0_GRANT_TYPE="`cat helm/da-affiliation/secrets/AUTH0_GRANT_TYPE.prod.secret`"
export AUTH0_CLIENT_ID="`cat helm/da-affiliation/secrets/AUTH0_CLIENT_ID.prod.secret`"
export AUTH0_CLIENT_SECRET="`cat helm/da-affiliation/secrets/AUTH0_CLIENT_SECRET.prod.secret`"
export AUTH0_AUDIENCE="`cat helm/da-affiliation/secrets/AUTH0_AUDIENCE.prod.secret`"
export AUTH0_TOKEN_ENDPOINT="`cat helm/da-affiliation/secrets/AUTH0_TOKEN_ENDPOINT.prod.secret`"
export SLACK_WEBHOOK_URL="`cat helm/da-affiliation/secrets/SLACK_WEBHOOK_URL.prod.secret`"
./sh/api.sh
