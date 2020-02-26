#!/bin/bash
if [ -z "$PASS" ]
then
  echo "$0: please specify MariaDB password via PASS=..."
  exit 1
fi
docker run -p 13306:3306 -e MYSQL_ROOT_PASSWORD="${PASS}" mariadb
