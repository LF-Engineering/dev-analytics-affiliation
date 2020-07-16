#!/bin/bash
export SKIP_PROJECT=1
. ./sh/shared.sh
for prop in da_name sf_name sf_id
do
  if [ ! -z "${!prop}" ]
  then
    encoded=$(rawurlencode "${!prop}")
    if [ -z "$extra" ]
    then
      extra="?$prop=${encoded}"
    else
      extra="${extra}&$prop=${encoded}"
    fi
  fi
done
if [ ! -z "$DEBUG" ]
then
  echo curl -i -s -H "Origin: ${ORIGIN}" -H "Authorization: Bearer ${JWT_TOKEN}" -XDELETE "${API_URL}/v1/affiliation/delete_slug_mapping${extra}"
  curl -i -s -H "Origin: ${ORIGIN}" -H "Authorization: Bearer ${JWT_TOKEN}" -XDELETE "${API_URL}/v1/affiliation/delete_slug_mapping${extra}"
else
  curl -s -H "Origin: ${ORIGIN}" -H "Authorization: Bearer ${JWT_TOKEN}" -XDELETE "${API_URL}/v1/affiliation/delete_slug_mapping${extra}"
fi
