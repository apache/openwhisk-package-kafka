#!/bin/bash
#
# use the command line interface to install standard actions deployed
# automatically
#
# To run this command
# ./installCatalog.sh  <AUTH> <APIHOST> <WSK_CLI>
# AUTH and APIHOST are found in $HOME/.wskprops
# WSK_CLI="$OPENWHISK_HOME/bin/wsk"

set -e
set -x

if [ $# -eq 0 ]
then
echo "Usage: ./installCatalog.sh <authkey> <apihost> <pathtowskcli>"
fi

AUTH="$1"
APIHOST="$2"
WSK_CLI="$3"

# If the auth key file exists, read the key in the file. Otherwise, take the
# first argument as the key itself.
if [ -f "$AUTH" ]; then
    AUTH=`cat $AUTH`
fi

PACKAGE_HOME="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

export WSK_CONFIG_FILE= # override local property file to avoid namespace clashes

echo Installing the kafka package and feed action.

# need to add annotations
$WSK_CLI -i --apihost "$APIHOST"  package update --auth "$AUTH"  --shared yes messaging

$WSK_CLI -i --apihost "$APIHOST" action update --auth "$AUTH" --shared yes messaging/feed "$PACKAGE_HOME/action/feedAction.js"
