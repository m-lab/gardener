#!/bin/bash
#
# apply-cluster.sh applies the k8s cluster configuration to the currently
# configured cluster. This script may be safely run multiple times to load the
# most recent configurations.
#
# Example:
#
#   PROJECT=mlab-sandbox CLUSTER=scraper-cluster ./apply-cluster.sh

set -x
set -e
set -u

USAGE="PROJECT=<projectid> CLUSTER=<cluster> $0"
PROJECT=${PROJECT:?Please provide project id: $USAGE}
CLUSTER=${CLUSTER:?Please provide cluster name: $USAGE}

# Check for per-project template variables.
if [[ ! -f "k8s/${CLUSTER}/${PROJECT}.yml" ]] ; then
  echo "No template variables found for k8s/${CLUSTER}/${PROJECT}.yml"
  # This is not necessarily an error, so exit cleanly.
  exit 0
fi

# Apply templates
CFG=/tmp/${CLUSTER}-${PROJECT}.yml
kexpand expand --ignore-missing-keys k8s/${CLUSTER}/*.yml \
    --value GCLOUD_PROJECT=${PROJECT} \
    --value GIT_COMMIT=${TRAVIS_COMMIT} \
    > ${CFG}

# This triggers deployment of the pod.
kubectl apply -f ${CFG}


# Get the public IP for the prometheus service.
PUBLIC_IP=$( kubectl get services \
  -o jsonpath='{.items[?(@.metadata.name=="etl-gardener-service")].status.loadBalancer.ingress[0].ip}' )
if [[ -n "${PUBLIC_IP}" ]] ; then
  # Reload configurations. If the deployment configuration has changed then this
  # request may fail because the container has already shutdown.
  curl -X POST http://${PUBLIC_IP}:8080/-/reload || :
fi
