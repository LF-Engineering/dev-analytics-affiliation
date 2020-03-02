#!/bin/bash
if [ -z "$ERROR_EXIT_CODE" ]
then
  ERROR_EXIT_CODE=1
fi
for f in `find . -maxdepth 4 -type f -iname "*.go" -not -path "./vendor/*"`
do
  if [ ! -z "$DEBUG" ]
  then
    echo $f
  fi
  if [ "$ERROR_EXIT_CODE" = "0" ]
  then
    $1 "$f"
  else
    $1 "$f" || exit $ERROR_EXIT_CODE
  fi
done
exit 0
