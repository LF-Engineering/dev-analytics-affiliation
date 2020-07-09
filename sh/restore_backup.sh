#!/bin/bash
if [ -z "$1" ]
then
  echo "$0: please provide day value 01-31"
  exit 1
fi
if [ -z "$ELB" ]
then
  echo "$0: please provide SH backups AWS ELB service URL via ELB=..."
  exit 2
fi
rm -f out.bz2 out && wget "${ELB}/backups/sortinghat-${1}.sql.bz2" -O out.bz2 && bzip2 -d out.bz2 && mv out sh/sh_full.sql && ./sh/mariadb.sh
