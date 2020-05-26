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

# use the command line interface to install standard actions deployed
# automatically

# To run this command
# ./installCatalog.sh <authkey> <edgehost> <dburl> <dbprefix> <apihost>
# authkey and apihost are found in $HOME/.wskprops

set -e
set -x

: ${OPENWHISK_HOME:?"OPENWHISK_HOME must be set and non-empty"}
WSK_CLI="$OPENWHISK_HOME/bin/wsk"

if [ $# -eq 0 ]
then
echo "Usage: ./installCatalog.sh <authkey> <edgehost> <dburl> <dbprefix> <apihost> <workers>"
fi

AUTH="$1"
EDGEHOST="$2"
DB_URL="$3"
DB_NAME="${4}ow_kafka_triggers"
APIHOST="$5"
WORKERS="$6"
INSTALL_PRODUCE_ACTION=${INSTALL_PRODUCE_ACTION:="true"}
ACTION_RUNTIME_VERSION=${ACTION_RUNTIME_VERSION:="nodejs:default"}

# If the auth key file exists, read the key in the file. Otherwise, take the
# first argument as the key itself.
if [ -f "$AUTH" ]; then
    AUTH=`cat $AUTH`
fi

# Make sure that the APIHOST is not empty.
: ${APIHOST:?"APIHOST must be set and non-empty"}

PACKAGE_HOME="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

export WSK_CONFIG_FILE= # override local property file to avoid namespace clashes

echo Installing the Message Hub package and feed action.

$WSK_CLI -i --apihost "$EDGEHOST" package update messaging \
    --auth "$AUTH" \
    --shared yes \
    -a parameters '[ {"name":"kafka_brokers_sasl", "required":true, "updatable":false, "description": "Array of Message Hub brokers", "bindTime":true},{"name":"user", "required":true, "updatable":false, "description": "Message Hub username", "bindTime":true},{"name":"password", "required":true, "updatable": false, "description": "Message Hub password", "bindTime":true, "type":"password"},{"name":"topic", "required":true, "updatable":false, "description": "Topic to subscribe to"},{"name":"isJSONData", "required":false, "updatable":true, "description": "Attempt to parse message value as JSON"},{"name":"isBinaryKey", "required":false, "updatable":true, "description": "Encode key as Base64"},{"name":"isBinaryValue", "required":false, "updatable":true, "description": "Encode message value as Base64"},{"name":"endpoint", "required":true, "updatable":false, "description": "Hostname and port of OpenWhisk deployment"},{"name":"kafka_admin_url", "required":true, "updatable":false, "description": "Your Message Hub admin REST URL", "bindTime":true}]' \
    -p bluemixServiceName 'messagehub' \
    -p endpoint "$APIHOST"

# make messageHubFeed.zip
OLD_PATH=`pwd`
cd action

if [ -e messageHubFeed.zip ]
then
    rm -rf messageHubFeed.zip
fi

cp -f messageHubFeed_package.json package.json
rm -rf node_modules
npm install
zip -r messageHubFeed.zip lib package.json messageHubFeed.js node_modules -q

$WSK_CLI -i --apihost "$EDGEHOST" action update --kind "$ACTION_RUNTIME_VERSION" messaging/messageHubFeed "$PACKAGE_HOME/action/messageHubFeed.zip" \
    --auth "$AUTH" \
    -a feed true \
    -a description 'Feed to list to Message Hub messages' \
    -a parameters '[ {"name":"kafka_brokers_sasl", "required":true, "description": "Array of Message Hub brokers"},{"name":"user", "required":true, "description": "Message Hub username"},{"name":"password", "required":true, "description": "Message Hub password", "type":"password"},{"name":"topic", "required":true, "description": "Topic to subscribe to"},{"name":"isJSONData", "required":false, "description": "Attempt to parse message value as JSON"},{"name":"isBinaryKey", "required":false, "description": "Encode key as Base64"},{"name":"isBinaryValue", "required":false, "description": "Encode message value as Base64"},{"name":"endpoint", "required":true, "description": "Hostname and port of OpenWhisk deployment"},{"name":"kafka_admin_url", "required":true, "description": "Your Message Hub admin REST URL"}]' \
    -a sampleInput '{"kafka_brokers_sasl":"[\"kafka01-prod01.messagehub.services.us-south.bluemix.net:9093\"]", "username":"someUsername", "password":"somePassword", "topic":"mytopic", "isJSONData": "false", "endpoint":"openwhisk.ng.bluemix.net", "kafka_admin_url":"https://kafka-admin-prod01.messagehub.services.us-south.bluemix.net:443"}'

# create messagingWeb package and web version of feed action
if [ -n "$WORKERS" ];
then
    $WSK_CLI -i --apihost "$EDGEHOST" package update messagingWeb \
        --auth "$AUTH" \
        --shared no \
        -p endpoint "$APIHOST" \
        -p DB_URL "$DB_URL" \
        -p DB_NAME "$DB_NAME"  \
        -p workers "$WORKERS"
else
    $WSK_CLI -i --apihost "$EDGEHOST" package update messagingWeb \
        --auth "$AUTH" \
        --shared no \
        -p endpoint "$APIHOST" \
        -p DB_URL "$DB_URL" \
        -p DB_NAME "$DB_NAME"
fi

# make messageHubFeedWeb.zip

if [ -e messageHubFeedWeb.zip ]
then
    rm -rf messageHubFeedWeb.zip
fi

cp -f messageHubFeedWeb_package.json package.json
rm -rf node_modules
npm install
zip -r messageHubFeedWeb.zip lib package.json messageHubFeedWeb.js node_modules -q

cd $OLD_PATH


$WSK_CLI -i --apihost "$EDGEHOST" action update --kind "$ACTION_RUNTIME_VERSION" messagingWeb/messageHubFeedWeb "$PACKAGE_HOME/action/messageHubFeedWeb.zip" \
    --auth "$AUTH" \
    --web true \
    -a description 'Write a new trigger to MH provider DB' \
    -a parameters '[ {"name":"kafka_brokers_sasl", "required":true, "description": "Array of Message Hub brokers"},{"name":"user", "required":true, "description": "Message Hub username"},{"name":"password", "required":true, "description": "Message Hub password", "type":"password"},{"name":"topic", "required":true, "description": "Topic to subscribe to"},{"name":"isJSONData", "required":false, "description": "Attempt to parse message value as JSON"},{"name":"isBinaryKey", "required":false, "description": "Encode key as Base64"},{"name":"isBinaryValue", "required":false, "description": "Encode message value as Base64"},{"name":"endpoint", "required":true, "description": "Hostname and port of OpenWhisk deployment"},{"name":"kafka_admin_url", "required":true, "description": "Your Message Hub admin REST URL"}]'

if [ $INSTALL_PRODUCE_ACTION == "true" ]; then
  $WSK_CLI -i --apihost "$EDGEHOST" action update messaging/messageHubProduce "$PACKAGE_HOME/action/messageHubProduce.py" \
      --auth "$AUTH" \
      --kind python:3 \
      -a deprecated true \
      -a description 'Deprecated - Produce a message to Message Hub' \
      -a parameters '[ {"name":"kafka_brokers_sasl", "required":true, "description": "Array of Message Hub brokers"},{"name":"user", "required":true, "description": "Message Hub username"},{"name":"password", "required":true, "description": "Message Hub password", "type":"password"},{"name":"topic", "required":true, "description": "Topic that you wish to produce a message to"},{"name":"value", "required":true, "description": "The value for the message you want to produce"},{"name":"key", "required":false, "description": "The key for the message you want to produce"},{"name":"base64DecodeValue", "required":false, "description": "If true, the message will be produced with a Base64 decoded version of the value parameter"},{"name":"base64DecodeKey", "required":false, "description": "If true, the message will be produced with a Base64 decoded version of the key parameter"}]' \
      -a sampleInput '{"kafka_brokers_sasl":"[\"kafka01-prod01.messagehub.services.us-south.bluemix.net:9093\"]", "username":"someUsername", "password":"somePassword", "topic":"mytopic", "value": "This is my message"}'
fi
