#!/bin/bash
if [ -z "$K" ]
then
  k="prodk.sh"
fi
> api_logs.txt
for po in `${k} -n da-affiliation get po -o json | jq -r '.items[].metadata.name'`
do
  echo $po
  ${k} -n da-affiliation logs "${po}" >> api_logs.txt
done
