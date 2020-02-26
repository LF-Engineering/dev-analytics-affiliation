#!/bin/bash
if [ "$1" = "docker" ]
then
  PASS=rootpwd ./sh/mariadb_local_docker.sh
else
  USR=root PASS=rootpwd SH_USR=sortinghat SH_PASS=pwd SH_DB=sortinghat FULL=1 ./sh/mariadb_reinit.sh
fi
