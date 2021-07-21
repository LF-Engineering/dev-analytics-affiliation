#!/bin/bash
TEST_ES_URL=`cat "./helm/da-affiliation/secrets/ELASTIC_URL.test.secret"`
PROD_ES_URL=`cat "./helm/da-affiliation/secrets/ELASTIC_URL.prod.secret"`
t=$(curl -s -XPOST -H 'Content-Type: application/json' "${TEST_ES_URL}/_sql?format=json" -d"{\"query\":\"select count(distinct hash) as commits from \\\"sds-cloud-foundry-cloud-foundry-git\\\" where origin = '$1'\",\"fetch_size\":10000}" | jq '.rows[0][0]')
p=$(curl -s -XPOST -H 'Content-Type: application/json' "${PROD_ES_URL}/_sql?format=json" -d"{\"query\":\"select count(distinct hash) as commits from \\\"sds-cloud-foundry-cloud-foundry-git\\\" where origin = '$1'\",\"fetch_size\":10000}" | jq '.rows[0][0]')
echo "test $1 commits: $t"
echo "prod $1 commits: $p"
