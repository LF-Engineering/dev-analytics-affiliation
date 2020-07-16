#!/bin/bash
export SKIP_PROJECT=1
export SKIP_TOKEN=1
. ./sh/shared.sh

if [ ! -z "$DEBUG" ]
then
  echo curl -i -s -H "Origin: https://insights.test.platform.linuxfoundation.org" -XGET "${API_URL}/v1/affiliation/list_slug_mappings"
  curl -i -s -H "Origin: ${ORIGIN}" -XGET "${API_URL}/v1/affiliation/list_slug_mappings"
else
  curl -s -H "Origin: ${ORIGIN}" -XGET "${API_URL}/v1/affiliation/list_slug_mappings"
fi

