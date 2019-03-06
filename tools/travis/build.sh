#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one or more contributor
# license agreements; and to You under the Apache License, Version 2.0.

set -e

# Build script for Travis-CI.

SCRIPTDIR=$(cd $(dirname "$0") && pwd)
ROOTDIR="$SCRIPTDIR/../.."
UTILDIR="$ROOTDIR/../incubator-openwhisk-utilities"

# run scancode
cd $UTILDIR
scancode/scanCode.py $ROOTDIR
