#!/bin/bash
#
# use the command line interface to install standard actions deployed
# automatically
#
# To run this command
# ./installCatalog.sh  <AUTH> <APIHOST> <KAFKA_TRIGGER_HOST> <KAFKA_TRIGGER_PORT>
# AUTH and APIHOST are found in $HOME/.wskprops

set -e
set -x

: ${OPENWHISK_HOME:?"OPENWHISK_HOME must be set and non-empty"}
WSK_CLI="$OPENWHISK_HOME/bin/wsk"

if [ $# -eq 0 ]
then
echo "Usage: ./installCatalog.sh <authkey> <apihost> <kafkatriggerhost> <kafkatriggerport>"
fi

AUTH="$1"
EDGEHOST="$2"
KAFKA_TRIGGER_HOST="$3"
KAFKA_TRIGGER_PORT="$4"
APIHOST="$5"


# If the auth key file exists, read the key in the file. Otherwise, take the
# first argument as the key itself.
if [ -f "$AUTH" ]; then
    AUTH=`cat $AUTH`
fi

# Make sure that the APIHOST is not empty.
: ${APIHOST:?"APIHOST must be set and non-empty"}

KAFKA_PROVIDER_ENDPOINT=$KAFKA_TRIGGER_HOST':'$KAFKA_TRIGGER_PORT

PACKAGE_HOME="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

export WSK_CONFIG_FILE= # override local property file to avoid namespace clashes

echo Installing the kafka feed action.

$WSK_CLI -i --apihost "$EDGEHOST" action update messaging/kafkaFeed "$PACKAGE_HOME/action/kafkaFeed.js" \
    --auth "$AUTH" \
    -a description 'Feed to listen to Kafka messages' \
    -a parameters '[ {"name":"brokers", "required":true, "description": "Array of Kafka brokers"}, {"name":"topic", "required":true, "description": "Topic to subscribe to"}, {"name":"isJSONData", "required":false, "description": "Attempt to parse message content as JSON"}, {"name":"endpoint", "required":true, "description": "Hostname and port of OpenWhisk deployment"}]' \
    -a sampleInput '{"brokers":"[\"127.0.0.1:9093\"]", "topic":"mytopic", "isJSONData":"false", "endpoint": "openwhisk.ng.bluemix.net"}'

$WSK_CLI -i --apihost "$EDGEHOST" action update messaging/kafkaProduce "$PACKAGE_HOME/action/kafkaProduce.py" \
    --auth "$AUTH" \
    -a description 'Produce a message to a Kafka cluster' \
    -a parameters '[ {"name":"brokers", "required":true, "description": "Array of Kafka brokers"},{"name":"topic", "required":true, "description": "Topic that you want to produce a message to"},{"name":"value", "required":true, "description": "The value for the message you want to produce"},{"name":"key", "required":false, "description": "The key for the message you want to produce"}]' \
    -a sampleInput '{"brokers":"[\"127.0.0.1:9093\"]", "topic":"mytopic", "value": "This is my message"}'
