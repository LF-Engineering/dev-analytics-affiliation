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
- After doing this you have MariaDB DSN that can be passed to API: `SH_DSN='sortinghat:pwd@tcp(localhost:13306)/sortinghat?charset=utf8'`.

Start local Postgres API DB instance:

- `PASS=postgrespwd ./psql_local_docker.sh`.
- `PASS=postgrespwd APIPASS=apipwd ./psql_init.sh`.
- You can also shell into dockerized DB instance: `PASS=postgrespwd ./psql_shell.sh`.
- You can also shell into dockerized DB instance (as an API user): `USR=lfda_api_user PASS=apipwd ./psql_api_shell.sh`.
- After doing this you have Postgres DSN that can be passed to API: ``.

Now run example API call:

- `SH_DSN='sortinghat:pwd@tcp(localhost:13306)/sortinghat?charset=utf8' ./dev-analytics-affiliation setOrgDomain Microsoft microsoft.com [overwrite [top]]`.
- `setOrgDomain` 3rd argument overwrite decided whatever force update existing affiliations to new domain or not touch them.
- `setOrgDomain` 4th argument can be used to set `is_top_domain` flag on `domains_organizations` table.
