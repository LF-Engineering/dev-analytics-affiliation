#!/bin/bash
curl -H "Content-Type: application/json" "${ES_URL}/sds-${1}-*,-*-raw,-*-for-merge/_search" -d @sh/top_contributors.json 2>/dev/null | jq
