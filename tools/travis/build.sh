#!/bin/bash
set -e

# Build script for Travis-CI.

SCRIPTDIR=$(cd $(dirname "$0") && pwd)
ROOTDIR="$SCRIPTDIR/../.."

cd $ROOTDIR
docker build -t kafka_feed_provider .
(docker run -i -t -e CLOUDANT_USER="" -e CLOUDANT_PASS="" -e LOCAL_DEV=true -e GENERIC_KAFKA=false -p 5000:5000 kafka_feed_provider) &
sleep 20
./gradlew tests:testProvider -Dhost=localhost -Dport=5000
