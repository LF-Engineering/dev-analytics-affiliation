# Code for affiliations related API

# Compilation

- `make setup`.
- `make swagger`.
- `make build`.
- `make run`.

# Testing locally

Start local SortingHat DB instance:

- `./sh/mariadb.sh docker`.
- Once docker instance is accepting connections: `./sh/mariadb.sh` to pupulate default data.
- You can shell into dockerized DB instance: `SH_USR=sortinghat SH_PASS=pwd SH_DB=sortinghat ./sh/mariadb_sortinghat_shell.sh`.
- After doing this you have MariaDB DSN that can be passed to API: `SH_DB_ENDPOINT='sortinghat:pwd@tcp(localhost:13306)/sortinghat?charset=utf8'`.

Start local Postgres API DB instance:

- `./sh/psql.sh docker`.
- Once docker instance is accepting connections: `./sh/psql.sh` to pupulate default data.
- You can also shell into dockerized DB instance: `PASS=postgrespwd ./sh/psql_shell.sh`.
- You can also shell into dockerized DB instance (as an API user): `USR=lfda_api_user PASS=apipwd ./sh/psql_api_shell.sh`.
- After doing this you have Postgres DSN that can be passed to API: `API_DB_ENDPOINT='host=127.0.0.1 user=lfda_api_user password=apipwd dbname=dev_analytics port=15432 sslmode=disable'`.
- To get most up-to-date API db follow (.gitignored): `secret/get_da_dump.md`.

Start local Elastic Search instance:

- `./sh/es.sh`.

# Start API server using

Start API server using dockerized MariaDB and Postgres databases:

- Start API server: `` [ONLYRUN=1] ./sh/api.sh ``. Eventually: `` ONLYRUN=1 NOCHECKS=1 ELASTIC_URL="`cat helm/da-affiliation/secrets/ELASTIC_URL.prod.secret`" ./sh/api.sh ``.
- Call example clients:
  - `` JWT_TOKEN=`cat secret/lgryglicki.token` ./sh/curl_put_org_domain.sh CNCF cncf.io 'odpi/egeria' 1 1 ``.
  - `` DEBUG=1 JWT_TOKEN=`cat secret/lgryglicki.token` ./sh/curl_put_merge_unique_identities.sh 'odpi/egeria' 16fe424acecf8d614d102fc0ece919a22200481d aaa8024197795de9b90676592772633c5cfcb35a [0] ``.
  - `` JWT_TOKEN=`cat secret/lgryglicki.token` ./sh/curl_put_move_identity.sh 'odpi/egeria' aaa8024197795de9b90676592772633c5cfcb35a 16fe424acecf8d614d102fc0ece919a22200481d [0] ``.
  - `` DEBUG=1 JWT_TOKEN=`cat secret/lgryglicki.token` ./sh/curl_get_matching_blacklist.sh 'odpi/egeria' root 5 1 ``.
  - `` JWT_TOKEN=`cat secret/lgryglicki.token` ./sh/curl_post_matching_blacklist.sh 'odpi/egeria' abc@xyz.ru ``.
  - `` JWT_TOKEN=`cat secret/lgryglicki.token` ./sh/curl_delete_matching_blacklist.sh 'odpi/egeria' abc@xyz.ru ``.
  - `` JWT_TOKEN=`cat secret/lgryglicki.token` ./sh/curl_get_list_organizations.sh odpi/egeria 'CNCF' 5 1 ``.
  - `` DEBUG='' JWT_TOKEN=`cat secret/lgryglicki.token` API_URL='http://127.0.0.1:18080' ./sh/curl_get_list_organizations.sh odpi/egeria 'google' 2>/dev/null | jq ``.
  - `` JWT_TOKEN=`cat secret/lgryglicki.token` ./sh/curl_get_unaffiliated.sh /projects/odpi/egeria 100 ``.
  - `` API_URL="`cat helm/da-affiliation/secrets/API_URL.prod.secret`" JWT_TOKEN=`cat secret/lgryglicki.token` ./sh/curl_get_unaffiliated.sh lfn/opnfv 100 2>/dev/null | jq ``.
  - `` JWT_TOKEN=`cat secret/lgryglicki.token` ./sh/curl_get_top_contributors.sh lfn 0 2552790984700 30 2>/dev/null | jq ``.
- Some special utils:
  - `` RAW=1 ES_URL=... ./sh/curl_es_unaffiliated.sh lfn/opnfv | jq .aggregations.unaffiliated.unaffiliated.buckets ``.
  - `` ES_URL="`cat helm/da-affiliation/secrets/API_URL.prod.secret`" ./sh/curl_es_unaffiliated.sh lfn/onap ``.
  - `` ES_URL="`cat helm/da-affiliation/secrets/ELASTIC_URL.prod.secret`" ./sh/curl_get_top_contributors_query.sh lfn ``.


# Docker

To deploy to docker:

- Build docker image: `DOCKER_USER=... docker/build_image.sh`.
- Run it: `DOCKER_USER=... docker/run.sh`. It will serve on 18080 instead of 8080 port.
- Test any api call, `API_URL` must be provided to specify non-default 18080 port: `` API_URL='http://127.0.0.1:18080' JWT_TOKEN=`cat secret/lgryglicki.token` ./sh/curl_get_matching_blacklist.sh 'odpi/egeria' root 5 1 ``.


# Kubernetes/Helm

To deploy on Kubernetes

- Go to `helm/`, run (LF real world example): `NODES=4 ./setup.sh prod`.
- Eventually adjust Helm chart to your needs, including `setup.sh` and `delete.sh` shell scripts.
- Run from repository root directory (test env): `` API_URL="`cat helm/da-affiliation/secrets/API_URL.test.secret`"  JWT_TOKEN=`cat secret/lgryglicki.test.token` ./sh/curl_get_list_organizations.sh darst '' 20 2 ``.
- Run from repository root directory (prod env): `` API_URL="`cat helm/da-affiliation/secrets/API_URL.prod.secret`"  JWT_TOKEN=`cat secret/lgryglicki.token` ./sh/curl_get_list_organizations.sh odpi/egeria '' 20 2 ``.


# Testing API

API is deployed on teh LF both test and prod LF Kubernetes clusters.

- You can get test API endpoint URL from `helm/da-affiliation/secrets/API_URL.test.secret` file or by running: `testk.sh -n da-affiliation get svc` - take EXTERNAL-IP column and add `:8080` port.
- You can get prod API endpoint URL from `helm/da-affiliation/secrets/API_URL.prod.secret` file or by running: `prodk.sh -n da-affiliation get svc` - take EXTERNAL-IP column and add `:8080` port.


# JWT token

- To actually execute any API call from commandline you will need JWT token (it expires after 24 hours), to get that token value you need to:
  - Go to lfanalytics.io or test.lfanalytics.io (depending which environment token is needed), sign out if your are signed in.
  - Sign in again.
  - Hit back buton in the browser - you will land on the authorization URL, copy that URL from the browser.
  - URL will be in format `` https://[redacted]/auth#access_token=....&id_token=XXXXXX&scope=...&expires_in=....&token_type=Bearer&state=... ``. Copy the `XXXXXX` value - this is your JWT_TOKEN valid for the next 24 hours.
  - Save token somewhere, for example in `token.secret` file.
  - Try any API via: `` API_URL="`cat helm/da-affiliation/secrets/API_URL.prod.secret`"  JWT_TOKEN=`cat token.secret` ./sh/curl_get_list_organizations.sh odpi/egeria google 2>/dev/null | jq ``.
  - You need to have permission to manage identities in the API database, if you don't have it you can login to test API database using `helm/da-affiliation/secrets/API_DB_ENDPOINT.test.secret` file to get database connect string.
  - Then: `PGPASSWORD=... psql -h db.host.com -U dbuser dbname` and add permissions for your user by running `sql/add_permissions.sql` query replacing `lgryglicki` username with you own username.
- In real deployment, you will always have that token available on the client side after signing in to the system.


# SortingHat

To debug what SortingHat package executes try:

- Start Grimoire base container: `docker run -it "lukaszgryglicki/dev-analytics-grimoire-docker-minimal" /bin/bash`.
- Run inside the container:
  - `cd /repos/grimoirelab-sortinghat/sortinghat`.
  - `vim api.py`, search for `/merge_unique_identities`.
  - Apply `import pdb` and `pdb.set_trace()` to observe.
  - Run: `sortinghat --host 172.17.0.1 --port 13306 -u sortinghat -p pwd -d sortinghat merge 16fe424acecf8d614d102fc0ece919a22200481d aaa8024197795de9b90676592772633c5cfcb35a`.
- You can shell to that container from another terminal:
  - `docker container ls` to get running container ID.
  - `docker exec -it b6d90ea6be2f /bin/bash`.
- Vim is auto selecting using console mouse which can be turned off (together with turning syntax on): `vim ~/.vimrc`:
```
set mouse=
syntax on
```
