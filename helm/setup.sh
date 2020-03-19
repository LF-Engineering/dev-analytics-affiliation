#!/bin/bash
# NODES=4 - set number of nodes
# DRY=1 - dry run mode
# NS=da-affiliation - set namespace name, default da-affiliation
helm=helm
denv=test
if [ -z "$1" ]
then
  echo "$0: you should env: test, prod, using default helm"
else
  helm="${1}h.sh"
  denv="${1}"
fi
if [ -z "$NODES" ]
then
  export NODES=1
fi
if [ -z "$NS" ]
then
  NS=da-affiliation
fi
if [ -z "$DRY" ]
then
  $helm install "${NS}-namespace" ./da-affiliation --set "namespace=$NS,skipSecrets=1,skipAPI=1,nodeNum=$NODES"
  change_namespace.sh $1 "$NS"
  $helm install "$NS" ./da-affiliation --set "namespace=$NS,deployEnv=$denv,skipNamespace=1,nodeNum=$NODES"
  change_namespace.sh $1 default
else
  echo "Dry run mode"
  change_namespace.sh $1 "$NS"
  $helm install --debug --dry-run --generate-name ./da-affiliation --set "namespace=$NS,deployEnv=$denv,nodeNum=$NODES,dryRun=1"
  change_namespace.sh $1 default
fi
