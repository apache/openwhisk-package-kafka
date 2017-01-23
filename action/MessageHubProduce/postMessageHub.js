/**
 *  Action to post a message to a Message Hub instance
 *  @param {string} kafka_brokers_sasl - array of Message Hub brokers
 *  @param {string} user - Message Hub username
 *  @param {string} password - Message Hub password
 *  @param {string} topic - topic to produce message
 *  @param {string} value - content of message
 *  @param {string} key - message key (optional)
 *  @param {int}    required_acks - number of acks required before action is complete (optional, default = 1)
 *  @param {bool}   isJSONData - attempt to parse messages as JSON
 */
function main(args) {
    return validateParameters(args)
        .then(produceMessage);
}

function produceMessage(args) {
    const produce = require('../lib/produce');

    var producerConfig = {
        "metadata.broker.list": args.kafka_brokers_sasl,
        "ssl.ca.location": "/etc/ssl/certs/",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": args.user,
        "sasl.password": args.password,
        "security.protocol": "sasl_ssl"
    };

    return produce(args, producerConfig);
}

function validateParameters(params) {
    const requiredParameters = ['kafka_brokers_sasl', 'user', 'password', 'topic', 'value'];
    const utils = require('../lib/utils');

    var missingParams = utils.findMissingKeys(requiredParameters, params);

    if(missingParams.length > 0) {
        return Promise.reject('You must supply all of the following required parameters: ' + missingParams.join(','));
    } else {
        return Promise.resolve(params);
    }
}

exports.main = main
