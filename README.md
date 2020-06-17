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

You also need to have `ssaw` deployment address (you can use one from `SYNC_URL.prod.secret`) - this is needed to trigger ssaw sync. You can set it to "xyz" if you don't have one, API will log sync error but this is not fatal.

# Start API server using

Start API server using dockerized MariaDB and Postgres databases:

- Start API server: `` ./sh/api.sh ``. Eventually: `` LOG_LEVEL=debug [N_CPUS=1|N|''] ONLYRUN=1 NOCHECKS=1 AUTH0_DOMAIN="`cat helm/da-affiliation/secrets/AUTH0_DOMAIN.prod.secret`" ELASTIC_URL="`cat helm/da-affiliation/secrets/ELASTIC_URL.prod.secret`" ./sh/api.sh ``.
- Call example clients:
  - `` JWT_TOKEN=`cat secret/lgryglicki.token` ./sh/curl_put_org_domain.sh 'odpi/egeria' CNCF cncf.io 1 1 0 ``.
  - `` DEBUG=1 JWT_TOKEN=`cat secret/lgryglicki.token` ./sh/curl_put_merge_unique_identities.sh 'odpi/egeria' 16fe424acecf8d614d102fc0ece919a22200481d aaa8024197795de9b90676592772633c5cfcb35a [0] ``.
  - `` JWT_TOKEN=`cat secret/lgryglicki.token` ./sh/curl_put_move_identity.sh 'odpi/egeria' aaa8024197795de9b90676592772633c5cfcb35a 16fe424acecf8d614d102fc0ece919a22200481d [0] ``.
  - `` DEBUG=1 JWT_TOKEN=`cat secret/lgryglicki.token` ./sh/curl_get_matching_blacklist.sh 'odpi/egeria' root 5 1 ``.
  - `` JWT_TOKEN=`cat secret/lgryglicki.token` ./sh/curl_post_matching_blacklist.sh 'odpi/egeria' abc@xyz.ru ``.
  - `` JWT_TOKEN=`cat secret/lgryglicki.token` ./sh/curl_delete_matching_blacklist.sh 'odpi/egeria' abc@xyz.ru ``.
  - `` JWT_TOKEN=`cat secret/lgryglicki.token` ./sh/curl_get_list_organizations.sh odpi/egeria 'CNCF' 5 1 ``.
  - `` JWT_TOKEN=`cat secret/lgryglicki.token` ./sh/curl_post_add_organization.sh odpi/egeria ABC ``.
  - `` JWT_TOKEN=`cat secret/lgryglicki.token` ./sh/curl_get_find_organization_by_id.sh odpi/egeria 28143 ``.
  - `` JWT_TOKEN=`cat secret/lgryglicki.token` ./sh/curl_get_find_organization_by_name.sh odpi/egeria CNCF ``.
  - `` DEBUG=1 ORIGIN=prod API_URL=prod JWT_TOKEN=`cat secret/lgryglicki.token` ./sh/curl_get_find_organization_by_name.sh lfn CNCF ``.
  - `` JWT_TOKEN=`cat secret/lgryglicki.token` ./sh/curl_put_edit_organization.sh odpi/egeria 28143 cncf ``.
  - `` DEBUG='' JWT_TOKEN=`cat secret/lgryglicki.token` API_URL='http://127.0.0.1:18080' ./sh/curl_get_list_organizations.sh odpi/egeria 'google' | jq ``.
  - `` JWT_TOKEN=`cat secret/lgryglicki.token` ./sh/curl_get_list_organizations_domains.sh odpi/egeria 28230 '.' 2 2 | jq ``.
  - `` JWT_TOKEN=`cat secret/lgryglicki.token` ./sh/curl_get_list_organizations_domains.sh odpi/egeria 0 'org' 0 | jq ``.
  - `` JWT_TOKEN=`cat secret/lgryglicki.token` ./sh/curl_get_unaffiliated.sh /projects/odpi/egeria 30 2 ``.
  - `` API_URL="`cat helm/da-affiliation/secrets/API_URL.prod.secret`" JWT_TOKEN=`cat secret/lgryglicki.token` ./sh/curl_get_unaffiliated.sh lfn/opnfv 100 | jq ``.
  - `` JWT_TOKEN=`cat secret/lgryglicki.token` ./sh/curl_get_top_contributors.sh lfn 0 2552790984700 30 2 '*john' git_commits desc | jq ``.
  - `` JWT_TOKEN=`cat secret/lgryglicki.token` ./sh/curl_get_top_contributors.sh lfn 0 1852790984700 5 0 'author*,*uuid*=*7b4d728ae99fd7c989a0ce3c7*' git_commits desc | jq ``.
  - `` JWT_TOKEN=`cat secret/lgryglicki.token` ./sh/curl_get_top_contributors.sh lfn 0 1852790984700 5 0 'all=*7b4*' git_commits desc | jq ``.
  - `` JWT_TOKEN=`cat secret/lgryglicki.token` ./sh/curl_get_top_contributors.sh lfn | jq ``.
  - `` JWT_TOKEN=`cat secret/lgryglicki.token` ./sh/curl_get_top_contributors_csv.sh lfn 0 2552790984700 300 2 john git_commits desc ``.
  - `` JWT_TOKEN=`cat secret/lgryglicki.token` ./sh/curl_get_top_contributors_csv.sh lfn 0 1852790984700 3 0 '*name,author*,*org*=*oogle*' git_commits desc ``.
  - `` JWT_TOKEN=`cat secret/lgryglicki.token` ./sh/curl_delete_org_domain.sh odpi/egeria cncf cloudnative.io ``.
  - `` JWT_TOKEN=`cat secret/lgryglicki.token` ./sh/curl_get_list_profiles.sh odpi/egeria gerrit 25 | jq ``.
  - `` JWT_TOKEN=`cat secret/lgryglicki.token` ./sh/curl_get_profile.sh lfn 16fe424acecf8d614d102fc0ece919a22200481d | jq ``.
  - `` JWT_TOKEN=`cat secret/lgryglicki.token` ./sh/curl_delete_profile.sh odpi/egeria xyz 1 | jq ``.
  - `` JWT_TOKEN=`cat secret/lgryglicki.token` ./sh/curl_unarchive_profile.sh odpi/egeria xyz | jq ``.
  - `` JWT_TOKEN=`cat secret/lgryglicki.token` name=lukaszgryglicki email=lgryglicki@cncf.io gender=male gender_acc=99 is_bot=0 country_code=pl ./sh/curl_put_edit_profile.sh odpi/egeria xyz ``.
  - `` JWT_TOKEN=`cat secret/lgryglicki.token` name='a' email=lgryglicki@cncf.io gender=male gender_acc=100 is_bot=0 country_code=BAD ./sh/curl_put_edit_profile.sh odpi/egeria xyz | jq ``.
  - `` JWT_TOKEN=`cat secret/lgryglicki.token` name='Lukasz Gryglicki' email='lgryglicki@cncf.io' username='' uuid='xyz' ./sh/curl_post_add_identity.sh odpi/egeria git ``.
  - `` JWT_TOKEN=`cat secret/lgryglicki.token` name='LukaszGryglicki' email='lgryglicki@cncf.io' username='Luki' uuid='' ./sh/curl_post_add_identity.sh odpi/egeria gitlab | jq ``.
  - `` JWT_TOKEN=`cat secret/lgryglicki.token` name='LGryglicki' email='lukaszgryglicki@cncf.io' username='LukiG' uuid='784f77c8a68d149376094cbac8421539196206cf' ./sh/curl_post_add_identity.sh odpi/egeria gitlab | jq ``.
  - `` JWT_TOKEN=`cat secret/lgryglicki.token` ./sh/curl_delete_identity.sh odpi/egeria 5d53a9a4774a912e19fc7afe4a21bcc0ea8a63bb ``.
  - `` JWT_TOKEN=`cat secret/lgryglicki.token` ./sh/curl_get_profile_enrollments.sh odpi/egeria aaa8024197795de9b90676592772633c5cfcb35a | jq ``.
  - `` JWT_TOKEN=`cat secret/lgryglicki.token` ./sh/curl_get_profile_enrollments.sh odpi/egeria 0000142135434a2b963c916185862168806fb1f5 ``.
  - `` JWT_TOKEN=`cat secret/lgryglicki.token` start='2015-05-05T15:15:05Z' end='2015-05-05T18:30:08Z' is_project_specific=true merge=1 ./sh/curl_post_add_enrollment.sh odpi/egeria 0000142135434a2b963c916185862168806fb1f5 CNCF ``.
  - `` JWT_TOKEN=`cat secret/lgryglicki.token` start='2015-05-05T15:15:05' end='2015-05-05T19:19' ./sh/curl_post_add_enrollment.sh odpi/egeria 0000142135434a2b963c916185862168806fb1f5 CNCF | jq ``.
  - `` JWT_TOKEN=`cat secret/lgryglicki.token` start='2012-08-01T00:00' end='2013-10-15T00:00' is_project_specific=true new_start='2011-01-01T00:00' new_end='2016-01-01T00:00' merge=1 new_is_project_specific=false ./sh/curl_put_edit_enrollment.sh odpi/egeria 16fe424acecf8d614d102fc0ece919a22200481d Cleverstep | jq ``.
  - `` JWT_TOKEN=`cat secret/lgryglicki.token` start='2000-01-01T00:00' end='2021-01-01T00:00' ./sh/curl_delete_enrollments.sh odpi/egeria 0000142135434a2b963c916185862168806fb1f5 CNCF | jq ``.
  - `` JWT_TOKEN=`cat secret/lgryglicki.test.token` is_project_specific=true ./sh/curl_delete_enrollments.sh project1 f1dd198c9d0427f603789b5a8cc7e0bc3ca66649 'Intel Corporation' | jq ``.
  - `` JWT_TOKEN=`cat secret/lgryglicki.token` ./sh/curl_delete_enrollment.sh project1 79523 | jq ``.
  - `` JWT_TOKEN=`cat secret/lgryglicki.token` is_project_specific=true ./sh/curl_put_merge_enrollments.sh proj1 0000142135434a2b963c916185862168806fb1f5 CNCF | jq ``.
  - `` JWT_TOKEN=`cat secret/lgryglicki.token` all_projects=true ./sh/curl_put_merge_enrollments.sh proj2 0000142135434a2b963c916185862168806fb1f5 'Intel Corporation' | jq ``.
  - `` JWT_TOKEN="`cat secret/lgryglicki.token`" ./sh/curl_put_merge_all.sh ``.
  - `` JWT_TOKEN="`cat secret/lgryglicki.token`" ./sh/curl_put_hide_emails.sh ``.
  - `` JWT_TOKEN="`cat secret/lgryglicki.token`" ./sh/curl_get_all_yaml.sh ``.
  - `` JWT_TOKEN="`cat secret/lgryglicki.token`" ./sh/curl_post_bulk_update.sh ``.
- Some special utils:
  - `` RAW=1 ES_URL=... ./sh/curl_es_unaffiliated.sh lfn/opnfv | jq .aggregations.unaffiliated.unaffiliated.buckets ``.
  - `` ES_URL="`cat helm/da-affiliation/secrets/API_URL.prod.secret`" ./sh/curl_es_unaffiliated.sh lfn/onap ``.
  - `` ES_URL="`cat helm/da-affiliation/secrets/ELASTIC_URL.prod.secret`" SEARCH=john SIZE=1 ./sh/curl_get_top_contributors_query.sh lfn ``.

# Docker

To deploy to docker:

- Build docker image: `DOCKER_USER=... ./docker/build_image.sh`.
- Run it: `DOCKER_USER=... [LOG_LEVEL=debug] [N_CPUS=16] ./docker/run.sh`. It will serve on 18080 instead of 8080 port. `N_CPUS` is optional, skipping will use auto-detecting, setting to 1 will enable singlethreaded mode.
- Test any api call, `API_URL` must be provided to specify non-default 18080 port: `` API_URL='http://127.0.0.1:18080' JWT_TOKEN=`cat secret/lgryglicki.token` ./sh/curl_get_matching_blacklist.sh 'odpi/egeria' root 5 1 ``.


# Kubernetes/Helm

To deploy on Kubernetes

- Go to `helm/`, run (LF real world example): `NODES=4 LOG_LEVEL=debug ./setup.sh prod`.
- Eventually adjust Helm chart to your needs, including `setup.sh` and `delete.sh` shell scripts.
- Run from repository root directory (test env): `` API_URL="`cat helm/da-affiliation/secrets/API_URL.test.secret`"  JWT_TOKEN=`cat secret/lgryglicki.test.token` ./sh/curl_get_list_organizations.sh darst '' 20 2 ``.
- Run from repository root directory (prod env): `` API_URL="`cat helm/da-affiliation/secrets/API_URL.prod.secret`"  JWT_TOKEN=`cat secret/lgryglicki.token` ./sh/curl_get_list_organizations.sh odpi/egeria '' 20 2 ``.


# Testing API

API is deployed on teh LF both test and prod LF Kubernetes clusters.

- You can get test API endpoint URL from `helm/da-affiliation/secrets/API_URL.test.secret` file or by running: `testk.sh -n da-affiliation get svc` - take EXTERNAL-IP column and add `:8080` port.
- You can get prod API endpoint URL from `helm/da-affiliation/secrets/API_URL.prod.secret` file or by running: `prodk.sh -n da-affiliation get svc` - take EXTERNAL-IP column and add `:8080` port.


# JWT token

- You can generate special API user token via `./sh/get_token.sh prod` (can be used for testing and to access DA-affiliation API from other services).
- To actually execute any API call from commandline you will need JWT token (it expires after 24 hours), to get that token value you need to:
  - Go to lfanalytics.io or insights.test.platform.linuxfoundation.org (depending which environment token is needed), sign out if your are signed in.
  - Sign in again.
  - Hit back buton in the browser - you will land on the authorization URL, copy that URL from the browser.
  - URL will be in format `` https://[redacted]/auth#access_token=....&id_token=XXXXXX&scope=...&expires_in=....&token_type=Bearer&state=... ``. Copy the `XXXXXX` value - this is your JWT_TOKEN valid for the next 24 hours.
  - Save token somewhere, for example in `token.secret` file.
  - Try any API via: `` API_URL="`cat helm/da-affiliation/secrets/API_URL.prod.secret`"  JWT_TOKEN=`cat token.secret` ./sh/curl_get_list_organizations.sh odpi/egeria google | jq ``.
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
