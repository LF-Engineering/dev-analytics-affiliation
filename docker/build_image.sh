#!/bin/bash
if [ -z "${DOCKER_USER}" ]
then
  echo "$0: you need to set docker user via DOCKER_USER=username"
  exit 1
fi

image="${DOCKER_USER}/dev-analytics-affiliation-api"
if [ ! -z "${1}" ]
then
  image="${image}-${1}"
  git checkout "${1}" || exit 5
fi

make docker || exit 2
docker build -f ./docker/Dockerfile -t "${image}" . || exit 3
docker push "${image}" || exit 4
echo OK
