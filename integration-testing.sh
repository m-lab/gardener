#!/bin/bash


# Install test credentials.
# The service account variables are uploaded to travis by running,
# from root of repo directory:
#  travis/setup_service_accounts_for_travis.sh
#
# All of the gcloud library calls will detect the GOOGLE_APPLICATION_CREDENTIALS
# environment variable, and use that file for authentication.
if [[ -z "$_SERVICE_ACCOUNT_MLAB_TESTING" ]] ; then
  echo "ERROR: testing service account is unavailable."
  exit 1
fi

gcloud config set project mlab-testing

echo "$_SERVICE_ACCOUNT_MLAB_TESTING" > $PWD/creds.json
# Make credentials available for Go libraries.
export GOOGLE_APPLICATION_CREDENTIALS=$PWD/creds.json
# Make credentials available for gcloud commands.
travis/activate_service_account.sh _SERVICE_ACCOUNT_MLAB_TESTING

# Rsync the mlab-testing ndt directory to match expected content.
pushd testfiles
./sync.sh
popd

# Start datastore emulator.
gcloud beta emulators datastore start --no-store-on-disk &

sleep 2 # allow time for emulator to start up.
$(gcloud beta emulators datastore env-init)
go test -v -tags=integration -coverprofile=_integration.cov ./...

# Shutdown datastore emulator.
kill %1
