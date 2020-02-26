#!/bin/bash
if [ "$1" = "docker" ]
then
  PASS=postgrespwd ./sh/psql_local_docker.sh
else
  PASS=postgrespwd APIPASS=apipwd SQL=1 ./sh/psql_init.sh
fi
