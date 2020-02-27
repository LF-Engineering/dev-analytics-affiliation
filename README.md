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

- Start API server: `` [ONLYRUN=1] ./sh/api.sh ``.
- Call example clients:
  - `` JWT_TOKEN=`cat secret/lgryglicki.token` ./sh/curl_put_org_domain.sh CNCF cncf.io 'odpi/egeria' 1 1 ``.
  - `` DEBUG=1 JWT_TOKEN=`cat secret/lgryglicki.token` ./sh/curl_put_merge_profiles.sh 'odpi/egeria' 16fe424acecf8d614d102fc0ece919a22200481d aaa8024197795de9b90676592772633c5cfcb35a ``.
  - `` JWT_TOKEN=`cat secret/lgryglicki.token` ./sh/curl_put_move_profile.sh 'odpi/egeria' aaa8024197795de9b90676592772633c5cfcb35a 16fe424acecf8d614d102fc0ece919a22200481d ``.
