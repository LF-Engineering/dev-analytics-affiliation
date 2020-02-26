#!/bin/bash
if ( [ -z "$PASS" ] || [ -z "$USR" ] || [ -z "$SH_PASS" ] || [ -z "$SH_USR" ] || [ -z "$SH_DB" ] )
then
  echo "$0: please specify MariaDB root user via USR=..."
  echo "$0: please specify MariaDB root password via PASS=..."
  echo "$0: please specify MariaDB Sorting Hat user via SH_USR=..."
  echo "$0: please specify MariaDB Sorting Hat user via SH_PASS=..."
  echo "$0: please specify MariaDB Sorting Hat database via SH_DB=..."
  exit 1
fi
./sh/mariadb_drop.sh
./sh/mariadb_init.sh
