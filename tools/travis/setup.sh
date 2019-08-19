#!/bin/bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

SCRIPTDIR=$(cd $(dirname "$0") && pwd)
HOMEDIR="$SCRIPTDIR/../../../"
WHISKDIR="$HOMEDIR/openwhisk"

sudo gpasswd -a travis docker
sudo -E bash -c 'echo '\''DOCKER_OPTS="-H tcp://0.0.0.0:4243 -H unix:///var/run/docker.sock --api-enable-cors --storage-driver=aufs"'\'' > /etc/default/docker'

# Docker
sudo apt-get -y update -qq
sudo apt-get -o Dpkg::Options::="--force-confold" --force-yes -y install docker-engine=1.12.0-0~trusty
sudo service docker restart
echo "Docker Version:"
docker version
echo "Docker Info:"
docker info

# Python
sudo apt-get -y install python-pip
pip install --user jsonschema

# Ansible
pip install --user ansible==2.1.2.0

# clone OpenWhisk repo. in order to run scanCode.py
cd $HOMEDIR
git clone https://github.com/apache/openwhisk-utilities.git

# OpenWhisk stuff
cd $HOMEDIR
git clone https://github.com/apache/openwhisk.git openwhisk
cd $WHISKDIR

TERM=dumb ./gradlew \
:common:scala:install \
:core:controller:install \
:core:invoker:install \
:tests:install
