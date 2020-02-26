#!/bin/bash
if ( [ -z "$PASS" ] || [ -z "$USR" ] )
then
  echo "$0: please specify MariaDB root user via USR=..."
  echo "$0: please specify MariaDB password via PASS=..."
  exit 1
fi
mysql -h127.0.0.1 -P13306 -p"${PASS}" -u"${USR}"
