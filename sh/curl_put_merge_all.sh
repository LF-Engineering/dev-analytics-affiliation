#!/bin/bash
export SKIP_PROJECT=1
. ./sh/shared.sh
if [ ! -z "$DEBUG" ]
then
  echo curl -i -s -H "Origin: ${ORIGIN}" -H "Authorization: Bearer ${JWT_TOKEN}" -XPUT "${API_URL}/v1/affiliation/merge_all"
  curl -i -s -H "Origin: ${ORIGIN}" -H "Authorization: Bearer ${JWT_TOKEN}" -XPUT "${API_URL}/v1/affiliation/merge_all"
else
  curl -s -H "Origin: ${ORIGIN}" -H "Authorization: Bearer ${JWT_TOKEN}" -XPUT "${API_URL}/v1/affiliation/merge_all"
fi
