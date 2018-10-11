#!/bin/bash

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
git clone https://github.com/apache/incubator-openwhisk-utilities.git

# OpenWhisk stuff
cd $HOMEDIR
git clone --depth=1 --single-branch -b scala-2-12 https://github.com/chetanmeh/incubator-openwhisk.git openwhisk
cd $WHISKDIR

TERM=dumb ./gradlew \
:common:scala:install \
:core:controller:install \
:core:invoker:install \
:tests:install
