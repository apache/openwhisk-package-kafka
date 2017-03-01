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

echo Installing the kafka package and feed action.

$WSK_CLI -i --apihost "$EDGEHOST" package update messaging \
    --auth "$AUTH" \
    --shared yes \
    -a parameters '[ {"name":"kafka_brokers_sasl", "required":true, "description": "Array of Message Hub brokers", "bindTime":true},{"name":"user", "required":true, "description": "Message Hub username", "bindTime":true},{"name":"password", "required":true, "description": "Message Hub password", "bindTime":true, "type":"password"},{"name":"topic", "required":true, "description": "Topic to subscribe to"},{"name":"isJSONData", "required":false, "description": "Attempt to parse message value as JSON"},{"name":"isBinaryKey", "required":false, "description": "Encode key as Base64"},{"name":"isBinaryValue", "required":false, "description": "Encode message value as Base64"},{"name":"endpoint", "required":true, "description": "Hostname and port of OpenWhisk deployment"},{"name":"kafka_admin_url", "required":true, "description": "Your Message Hub admin REST URL", "bindTime":true}]' \
    -p bluemixServiceName 'messagehub' \
    -p endpoint "$APIHOST" \
    -p package_endpoint $KAFKA_PROVIDER_ENDPOINT

$WSK_CLI -i --apihost "$EDGEHOST" action update messaging/messageHubFeed "$PACKAGE_HOME/action/messageHubFeed.js" \
    --auth "$AUTH" \
    -a feed true \
    -a description 'Feed to list to Message Hub messages' \
    -a parameters '[ {"name":"kafka_brokers_sasl", "required":true, "description": "Array of Message Hub brokers"},{"name":"user", "required":true, "description": "Message Hub username"},{"name":"password", "required":true, "description": "Message Hub password", "type":"password"},{"name":"topic", "required":true, "description": "Topic to subscribe to"},{"name":"isJSONData", "required":false, "description": "Attempt to parse message value as JSON"},{"name":"isBinaryKey", "required":false, "description": "Encode key as Base64"},{"name":"isBinaryValue", "required":false, "description": "Encode message value as Base64"},{"name":"endpoint", "required":true, "description": "Hostname and port of OpenWhisk deployment"},{"name":"kafka_admin_url", "required":true, "description": "Your Message Hub admin REST URL"}]' \
    -a sampleInput '{"kafka_brokers_sasl":"[\"kafka01-prod01.messagehub.services.us-south.bluemix.net:9093\"]", "username":"someUsername", "password":"somePassword", "topic":"mytopic", "isJSONData": "false", "endpoint":"openwhisk.ng.bluemix.net", "kafka_admin_url":"https://kafka-admin-prod01.messagehub.services.us-south.bluemix.net:443"}'

$WSK_CLI -i --apihost "$EDGEHOST" action update messaging/messageHubProduce "$PACKAGE_HOME/action/messageHubProduce.py" \
    --auth "$AUTH" \
    -a description 'Produce a message to Message Hub' \
    -a parameters '[ {"name":"kafka_brokers_sasl", "required":true, "description": "Array of Message Hub brokers"},{"name":"user", "required":true, "description": "Message Hub username"},{"name":"password", "required":true, "description": "Message Hub password", "type":"password"},{"name":"topic", "required":true, "description": "Topic that you wish to produce a message to"},{"name":"value", "required":true, "description": "The value for the message you want to produce"},{"name":"key", "required":false, "description": "The key for the message you want to produce"},{"name":"base64DecodeValue", "required":false, "description": "If true, the message will be produced with a Base64 decoded version of the value parameter"},{"name":"base64DecodeKey", "required":false, "description": "If true, the message will be produced with a Base64 decoded version of the key parameter"}]' \
    -a sampleInput '{"kafka_brokers_sasl":"[\"kafka01-prod01.messagehub.services.us-south.bluemix.net:9093\"]", "username":"someUsername", "password":"somePassword", "topic":"mytopic", "value": "This is my message"}'
