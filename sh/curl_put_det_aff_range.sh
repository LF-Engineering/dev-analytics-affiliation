#!/bin/bash
export SKIP_PROJECT=1
. ./sh/shared.sh
if [ ! -z "$DEBUG" ]
then
  echo curl -i -s -H "Origin: ${ORIGIN}" -H "Authorization: Bearer ${JWT_TOKEN}" -XPUT "${API_URL}/v1/affiliation/det_aff_range"
  curl -i -s -H "Origin: ${ORIGIN}" -H "Authorization: Bearer ${JWT_TOKEN}" -XPUT "${API_URL}/v1/affiliation/det_aff_range"
else
  curl -s -H "Origin: ${ORIGIN}" -H "Authorization: Bearer ${JWT_TOKEN}" -XPUT "${API_URL}/v1/affiliation/det_aff_range"
fi
