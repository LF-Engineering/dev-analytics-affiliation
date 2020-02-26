#!/bin/bash
if ( [ -z "$PASS" ] || [ -z "$USR" ] || [ -z "$SH_USR" ] || [ -z "$SH_DB" ] )
then
  echo "$0: please specify MariaDB root user via USR=..."
  echo "$0: please specify MariaDB root password via PASS=..."
  echo "$0: please specify MariaDB Sorting Hat user via SH_USR=..."
  echo "$0: please specify MariaDB Sorting Hat database via SH_DB=..."
  exit 1
fi

echo "drop database ``${SH_DB}``;" | mysql -h127.0.0.1 -P13306 -p"${PASS}" -u"${USR}"
echo "drop user '$SH_USR';" | mysql -h127.0.0.1 -P13306 -p"${PASS}" -u"${USR}"
