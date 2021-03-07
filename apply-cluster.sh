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
DATE_SKIP=${DATE_SKIP:-"0"}  # Number of dates to skip between each processed date (for sandbox).
TASK_FILE_SKIP=${TASK_FILE_SKIP:-"0"}  # Number of files to skip between each processed file (for sandbox).

# Create the configmap
kubectl create configmap gardener-config --dry-run \
    --from-file config/config.yml \
    -o yaml > k8s/data-processing/deployments/config.yml

# Apply templates
find k8s/${CLUSTER}/ -type f -exec \
    sed -i \
      -e 's/GIT_COMMIT/'${GIT_COMMIT}'/g' \
      -e 's/GCLOUD_PROJECT/'${PROJECT}'/g' \
      -e 's/DATE_SKIP/'${DATE_SKIP}'/g' \
      -e 's/TASK_FILE_SKIP/'${TASK_FILE_SKIP}'/g' \
      {} \;

# This triggers deployment of the pod.
kubectl apply --recursive -f k8s/${CLUSTER}
