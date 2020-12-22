#!/bin/bash
if ( [ -z "$SH_PASS" ] || [ -z "$SH_USR" ] || [ -z "$SH_DB" ] )
then
  echo "$0: please specify MariaDB read only user via SH_RO_USR=..."
  echo "$0: please specify MariaDB read only password via SH_PASS=..."
  echo "$0: please specify MariaDB read only database via SH_DB=..."
  exit 1
fi
echo mysql -h127.0.0.1 -P13306 -p"${SH_PASS}" -u"${SH_RO_USR}" "${SH_DB}"
mysql -h127.0.0.1 -P13306 -p"${SH_PASS}" -u"${SH_RO_USR}" "${SH_DB}"
