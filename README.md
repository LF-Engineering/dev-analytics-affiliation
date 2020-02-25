# Code for affiliations related API

# Compilation

- `make setup`.
- `make swagger`.
- `make build`.
- `make run`.

# Testing locally

Start local SortingHat DB instance by following [this](https://github.com/LF-Engineering/dev-analytics-import-sh-json#usage):

- `PASS=rootpwd ./mariadb_local_docker.sh`.
- `USR=root PASS=rootpwd SH_USR=sortinghat SH_PASS=pwd SH_DB=sortinghat FULL=1 ./mariadb_init.sh`.
- You can also shell into dockerized DB instance: `SH_USR=sortinghat SH_PASS=pwd SH_DB=sortinghat ./mariadb_sortinghat_shell.sh`.
- After doing this you have MariaDB DSN that can be passed to API: `SH_DB_ENDPOINT='sortinghat:pwd@tcp(localhost:13306)/sortinghat?charset=utf8'`.

Start local Postgres API DB instance:

- `PASS=postgrespwd ./psql_local_docker.sh`.
- `PASS=postgrespwd APIPASS=apipwd ./psql_init.sh`.
- You can also shell into dockerized DB instance: `PASS=postgrespwd ./psql_shell.sh`.
- You can also shell into dockerized DB instance (as an API user): `USR=lfda_api_user PASS=apipwd ./psql_api_shell.sh`.
- After doing this you have Postgres DSN that can be passed to API: `API_DB_ENDPOINT='host=127.0.0.1 user=lfda_api_user password=apipwd dbname=dev_analytics port=15432 sslmode=disable'`.

# Start API server using

Start API server using dockerized MariaDB and Postgres databases:

- Start API server: `` API_DB_ENDPOINT='host=127.0.0.1 user=lfda_api_user password=apipwd dbname=dev_analytics port=15432 sslmode=disable' SH_DB_ENDPOINT='sortinghat:pwd@tcp(localhost:13306)/sortinghat?charset=utf8' AUTH0_DOMAIN=`cat auth0.domain` make run ``.
- Call example client: `` JWT_TOKEN=`cat lgryglicki.token` ./curl.sh CNCF cncf.io 1 1 ``.
