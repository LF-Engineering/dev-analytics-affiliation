#!/bin/bash
if ( [ -z "${SKIP_TOKEN}" ] && [ -z "$JWT_TOKEN" ] )
then
  echo "$0: please sepcify your JWT token via JWT_TOKEN=..."
  exit 1
fi

if ( [ -z "${SKIP_PROJECT}" ] && [ -z "$1" ] )
then
  echo "$0: please specify project slug(s) as a 1st arg, example 'onap,opnfv,burrow,aries'"
  exit 2
fi

if [ -z "$API_URL" ]
then
  export API_URL="http://127.0.0.1:8080"
fi
if [ "$API_URL" = "prod" ]
then
  export API_URL="`cat helm/da-affiliation/secrets/API_URL.prod.secret`"
fi
if [ "$API_URL" = "test" ]
then
  export API_URL="`cat helm/da-affiliation/secrets/API_URL.test.secret`"
fi

if [ -z "$ORIGIN" ]
then
  export ORIGIN="http://127.0.0.1"
fi
if [ "$ORIGIN" = "prod" ]
then
  export ORIGIN='https://lfanalytics.io'
fi
if [ "$ORIGIN" = "test" ]
then
  export ORIGIN='https://insights.test.platform.linuxfoundation.org'
fi


rawurlencode() {
  local string="${1}"
  local strlen=${#string}
  local encoded=""
  local pos c o
  for (( pos=0 ; pos<strlen ; pos++ )); do
     c=${string:$pos:1}
     case "$c" in
        [-_.~a-zA-Z0-9] ) o="${c}" ;;
        * )               printf -v o '%%%02x' "'$c"
     esac
     encoded+="${o}"
  done
  echo "${encoded}"
  REPLY="${encoded}"
}

project=$(rawurlencode "${1}")
