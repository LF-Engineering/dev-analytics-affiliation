#!/bin/bash
# DOCKER_USER=lukaszgryglicki SKIP_TEST=1 SKIP_PROD=1 SKIP_FULL=1 SKIP_MIN=1 SKIP_GRAFANA=1 SKIP_TESTS=1 SKIP_PATRONI=1 SKIP_STATIC=1 SKIP_REPORTS=1 SKIP_API=1 SKIP_PUSH=1 ./images/build_images.sh
if [ -z "${DOCKER_USER}" ]
then
  echo "$0: you need to set docker user via DOCKER_USER=username"
  exit 1
fi

make docker || exit 2
docker build -f ./docker/Dockerfile -t "${DOCKER_USER}/dev-analytics-affiliation-api" . || exit 3
docker push "${DOCKER_USER}/dev-analytics-affiliation-api" || exit 4
echo OK
