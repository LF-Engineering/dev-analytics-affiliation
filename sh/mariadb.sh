#!/bin/bash
# UPDATE_STRUCTURE=1 - run update structure SQL
# TESTING_API=1 - add data useful for testing merge/unmerge API
if [ "$1" = "docker" ]
then
  PASS=rootpwd ./sh/mariadb_local_docker.sh
else
  USR=root PASS=rootpwd SH_USR=sortinghat SH_RO_USR=ro_user SH_PASS=pwd SH_DB=sortinghat ./sh/mariadb_reinit.sh
fi
