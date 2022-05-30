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
# ./installKafka.sh <authkey> <edgehost> <dburl> <dbprefix> <apihost>
# authkey and apihost are found in $HOME/.wskprops

set -e
set -x

: ${OPENWHISK_HOME:?"OPENWHISK_HOME must be set and non-empty"}
WSK_CLI="$OPENWHISK_HOME/bin/wsk"

if [ $# -eq 0 ]
then
echo "Usage: ./installKafka.sh <authkey> <edgehost> <dburl> <dbprefix> <apihost>"
fi

AUTH="$1"
EDGEHOST="$2"
DB_URL="$3"
DB_NAME="${4}ow_kafka_triggers"
APIHOST="$5"
WORKERS="$6"

# If the auth key file exists, read the key in the file. Otherwise, take the
# first argument as the key itself.
if [ -f "$AUTH" ]; then
    AUTH=`cat $AUTH`
fi

# Make sure that the APIHOST is not empty.
: ${APIHOST:?"APIHOST must be set and non-empty"}

PACKAGE_HOME="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

export WSK_CONFIG_FILE= # override local property file to avoid namespace clashes

echo Installing the Kafka package and feed action.

$WSK_CLI -i --apihost "$EDGEHOST" package update messaging \
    --auth "$AUTH" \
    --shared yes \
    -p endpoint "$APIHOST"

# make kafkaFeed.zip
OLD_PATH=`pwd`
cd action

if [ -e kafkaFeed.zip ]
then
    rm -rf kafkaFeed.zip
fi

cp -f kafkaFeed_package.json package.json
rm -rf node_modules
npm install
zip -r kafkaFeed.zip lib package.json kafkaFeed.js node_modules
cd $OLD_PATH

$WSK_CLI -i --apihost "$EDGEHOST" action update --kind nodejs:default messaging/kafkaFeed "$PACKAGE_HOME/action/kafkaFeed.zip" \
    --auth "$AUTH" \
    -a feed true \
    -a description 'Feed to listen to Kafka messages' \
    -a parameters '[ {"name":"brokers", "required":true, "updatable":false, "description": "Array of Kafka brokers"}, {"name":"topic", "required":true, "updatable":false, "description": "Topic to subscribe to"}, {"name":"isJSONData", "required":false, "updatable":true, "description": "Attempt to parse message value as JSON"}, {"name":"isBinaryKey", "required":false, "updatable":true, "description": "Encode key as Base64"}, {"name":"isBinaryValue", "required":false, "updatable":true, "description": "Encode message value as Base64"}, {"name":"endpoint", "required":true, "updatable":false, "description": "Hostname and port of OpenWhisk deployment"}]' \
    -a sampleInput '{"brokers":"[\"127.0.0.1:9093\"]", "topic":"mytopic", "isJSONData":"false", "endpoint": "openwhisk.ng.bluemix.net"}'

# create messagingWebDedicated package and web version of feed action
$WSK_CLI -i --apihost "$EDGEHOST" package update messagingWebDedicated \
    --auth "$AUTH" \
    --shared no \
    -p endpoint "$APIHOST"

# rebind package
$WSK_CLI -i --apihost "$EDGEHOST" package delete messagingWeb --auth "$AUTH"
if [ -n "$WORKERS" ];
then
    $WSK_CLI -i --apihost "$EDGEHOST" package bind messagingWebDedicated messagingWeb \
        --auth "$AUTH" \
        -p endpoint "$APIHOST" \
        -p DB_URL "$DB_URL" \
        -p DB_NAME "$DB_NAME"  \
        -p workers "$WORKERS"
else
    $WSK_CLI -i --apihost "$EDGEHOST" package bind messagingWebDedicated messagingWeb \
        --auth "$AUTH" \
        -p endpoint "$APIHOST" \
        -p DB_URL "$DB_URL" \
        -p DB_NAME "$DB_NAME"
fi

# make kafkaFeedWeb.zip
OLD_PATH=`pwd`
cd action

if [ -e kafkaFeedWeb.zip ]
then
    rm -rf kafkaFeedWeb.zip
fi

cp -f kafkaFeedWeb_package.json package.json
rm -rf node_modules
npm install
zip -r kafkaFeedWeb.zip lib package.json kafkaFeedWeb.js node_modules

cd $OLD_PATH


$WSK_CLI -i --apihost "$EDGEHOST" action update --kind nodejs:default messagingWebDedicated/kafkaFeedWeb "$PACKAGE_HOME/action/kafkaFeedWeb.zip" \
    --auth "$AUTH" \
    --web true \
    -a description 'Write a new trigger to Kafka provider DB' \
    -a parameters '[ {"name":"brokers", "required":true, "description": "Array of Kafka brokers"},{"name":"topic", "required":true, "description": "Topic to subscribe to"},{"name":"isJSONData", "required":false, "description": "Attempt to parse message value as JSON"},{"name":"isBinaryKey", "required":false, "description": "Encode key as Base64"},{"name":"isBinaryValue", "required":false, "description": "Encode message value as Base64"},{"name":"endpoint", "required":true, "description": "Hostname and port of OpenWhisk deployment"}]'

$WSK_CLI -i --apihost "$EDGEHOST" action update messaging/kafkaProduce "$PACKAGE_HOME/action/kafkaProduce.py" \
    --auth "$AUTH" \
    --kind python:3 \
    -a deprecated true \
    -a description 'Deprecated - Produce a message to a Kafka cluster' \
    -a parameters '[ {"name":"brokers", "required":true, "description": "Array of Kafka brokers"},{"name":"topic", "required":true, "description": "Topic that you want to produce a message to"},{"name":"value", "required":true, "description": "The value for the message you want to produce"},{"name":"key", "required":false, "description": "The key for the message you want to produce"},{"name":"base64DecodeValue", "required":false, "description": "If true, the message will be produced with a Base64 decoded version of the value parameter"},{"name":"base64DecodeKey", "required":false, "description": "If true, the message will be produced with a Base64 decoded version of the key parameter"}]' \
    -a sampleInput '{"brokers":"[\"127.0.0.1:9093\"]", "topic":"mytopic", "value": "This is my message"}'
