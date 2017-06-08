#!/bin/bash
set -e

# Build script for Travis-CI.

SCRIPTDIR=$(cd $(dirname "$0") && pwd)
ROOTDIR="$SCRIPTDIR/../.."
UTILDIR="$ROOTDIR/../incubator-openwhisk-utilities"

# run scancode
cd $UTILDIR
scancode/scanCode.py $ROOTDIR
