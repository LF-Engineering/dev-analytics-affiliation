#!/bin/bash
if [ -z "${ES}" ]
then
  echo "$0: ES env variable must be set"
  exit 1
fi
echo -n 'Devops Mutualization -> DevOps Mutualization: ' && curl -s -XPOST -H 'Content-type: application/json' "${ES}/sds-finos-*,-*-raw/_update_by_query?conflicts=proceed" -d"{\"query\":{\"term\":{\"project\":\"Devops Mutualization\"}},\"script\":\"ctx._source.project='DevOps Mutualization'\"}" | jq -rS '.updated'
echo -n 'InnerSource SIG -> InnerSource Special Interest Group: ' && curl -s -XPOST -H 'Content-type: application/json' "${ES}/sds-finos-*,-*-raw/_update_by_query?conflicts=proceed" -d"{\"query\":{\"term\":{\"project\":\"InnerSource SIG\"}},\"script\":\"ctx._source.project='InnerSource Special Interest Group'\"}" | jq -rS '.updated'
echo -n 'kdbplus -> kdb+: ' && curl -s -XPOST -H 'Content-type: application/json' "${ES}/sds-finos-*,-*-raw/_update_by_query?conflicts=proceed" -d"{\"query\":{\"term\":{\"project\":\"kdbplus\"}},\"script\":\"ctx._source.project='kdb+'\"}" | jq -rS '.updated'
echo -n 'ODP Project -> Open Developer Platform: ' && curl -s -XPOST -H 'Content-type: application/json' "${ES}/sds-finos-*,-*-raw/_update_by_query?conflicts=proceed" -d"{\"query\":{\"term\":{\"project\":\"ODP Project\"}},\"script\":\"ctx._source.project='Open Developer Platform'\"}" | jq -rS '.updated'
echo -n 'openmama -> OpenMAMA: ' && curl -s -XPOST -H 'Content-type: application/json' "${ES}/sds-finos-*,-*-raw/_update_by_query?conflicts=proceed" -d"{\"query\":{\"term\":{\"project\":\"openmama\"}},\"script\":\"ctx._source.project='OpenMAMA'\"}" | jq -rS '.updated'
echo -n 'Cloud Service Certification -> Compliant Financial Infrastructure: ' && curl -s -XPOST -H 'Content-type: application/json' "${ES}/sds-finos-*,-*-raw/_update_by_query?conflicts=proceed" -d"{\"query\":{\"term\":{\"project\":\"Cloud Service Certification\"}},\"script\":\"ctx._source.project='Compliant Financial Infrastructure'\"}" | jq -rS '.updated'
echo -n 'Alloy -> Legend: ' && curl -s -XPOST -H 'Content-type: application/json' "${ES}/sds-finos-*,-*-raw/_update_by_query?conflicts=proceed" -d"{\"query\":{\"term\":{\"project\":\"Alloy\"}},\"script\":\"ctx._source.project='Legend'\"}" | jq -rS '.updated'
echo -n 'Decentralized Ecosystem Growth -> FINOS Community: ' && curl -s -XPOST -H 'Content-type: application/json' "${ES}/sds-finos-*,-*-raw/_update_by_query?conflicts=proceed" -d"{\"query\":{\"term\":{\"project\":\"Decentralized Ecosystem Growth\"}},\"script\":\"ctx._source.project='FINOS Community'\"}" | jq -rS '.updated'
echo -n 'FINOS -> FINOS Community: ' && curl -s -XPOST -H 'Content-type: application/json' "${ES}/sds-finos-finos-community-slack/_update_by_query?conflicts=proceed" -d"{\"query\":{\"term\":{\"project\":\"FINOS\"}},\"script\":\"ctx._source.project='FINOS Community'\"}" | jq -rS '.updated'
