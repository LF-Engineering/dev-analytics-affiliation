#!/bin/bash
export SKIP_PROJECT=1
. ./sh/shared.sh
debug=$(rawurlencode "${1}")
dry=$(rawurlencode "${2}")
if [ ! -z "$DEBUG" ]
then
  echo curl -i -s -H "Origin: ${ORIGIN}" -H "Authorization: Bearer ${JWT_TOKEN}" -XPUT "${API_URL}/v1/affiliation/merge_all?debug=${debug}&dry=${dry}"
  curl -i -s -H "Origin: ${ORIGIN}" -H "Authorization: Bearer ${JWT_TOKEN}" -XPUT "${API_URL}/v1/affiliation/merge_all?debug=${debug}&dry=${dry}"
else
  curl -s -H "Origin: ${ORIGIN}" -H "Authorization: Bearer ${JWT_TOKEN}" -XPUT "${API_URL}/v1/affiliation/merge_all?debug=${debug}&dry=${dry}"
fi
