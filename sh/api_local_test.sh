#!/bin/bash
export LOG_LEVEL=info
export STAGE=test
export ONLYRUN=1
export NOCHECKS=1
export AUTH0_DOMAIN="`cat helm/da-affiliation/secrets/AUTH0_DOMAIN.test.secret`"
export ELASTIC_URL="`cat helm/da-affiliation/secrets/ELASTIC_URL.test.secret`"
export SH_DB_ENDPOINT="`cat helm/da-affiliation/secrets/SH_DB_ENDPOINT.test.secret`"
export SH_DB_RO_ENDPOINT="`cat helm/da-affiliation/secrets/SH_DB_ENDPOINT.test.secret`"
export API_DB_ENDPOINT="`cat helm/da-affiliation/secrets/API_DB_ENDPOINT.test.secret`"
export PLATFORM_USER_SERVICE_ENDPOINT="`cat helm/da-affiliation/secrets/PLATFORM_USER_SERVICE_ENDPOINT.test.secret`"
export PLATFORM_ORG_SERVICE_ENDPOINT="`cat helm/da-affiliation/secrets/PLATFORM_ORG_SERVICE_ENDPOINT.test.secret`"
export ELASTIC_CACHE_URL="`cat helm/da-affiliation/secrets/ELASTIC_CACHE_URL.test.secret`"
export ELASTIC_CACHE_USERNAME="`cat helm/da-affiliation/secrets/ELASTIC_CACHE_USERNAME.test.secret`"
export ELASTIC_CACHE_PASSWORD="`cat helm/da-affiliation/secrets/ELASTIC_CACHE_PASSWORD.test.secret`"
export AUTH0_GRANT_TYPE="`cat helm/da-affiliation/secrets/AUTH0_GRANT_TYPE.test.secret`"
export AUTH0_CLIENT_ID="`cat helm/da-affiliation/secrets/AUTH0_CLIENT_ID.test.secret`"
export AUTH0_CLIENT_SECRET="`cat helm/da-affiliation/secrets/AUTH0_CLIENT_SECRET.test.secret`"
export AUTH0_AUDIENCE="`cat helm/da-affiliation/secrets/AUTH0_AUDIENCE.test.secret`"
export AUTH0_TOKEN_ENDPOINT="`cat helm/da-affiliation/secrets/AUTH0_TOKEN_ENDPOINT.test.secret`"
export SLACK_WEBHOOK_URL="`cat helm/da-affiliation/secrets/SLACK_WEBHOOK_URL.test.secret`"
./sh/api.sh
