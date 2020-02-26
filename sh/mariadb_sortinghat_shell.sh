#!/bin/bash
if ( [ -z "$SH_PASS" ] || [ -z "$SH_USR" ] || [ -z "$SH_DB" ] )
then
  echo "$0: please specify MariaDB sortinghat user via SH_USR=..."
  echo "$0: please specify MariaDB sortinghat password via SH_PASS=..."
  echo "$0: please specify MariaDB sortinghat database via SH_DB=..."
  exit 1
fi
mysql -h127.0.0.1 -P13306 -p"${SH_PASS}" -u"${SH_USR}" "${SH_DB}"
