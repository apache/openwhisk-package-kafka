const common = require('./lib/common');

/**
 *   Feed to listen to Kafka messages
 *  @param {string} brokers - array of Kafka brokers
 *  @param {string} topic - topic to subscribe to
 *  @param {bool}   isJSONData - attempt to parse messages as JSON
 *  @param {bool}   isBinaryKey - encode key as Base64
 *  @param {bool}   isBinaryValue - encode message as Base64
 *  @param {string} endpoint - address to OpenWhisk deployment (expected to be bound at deployment)
 */
function main(params) {
    const endpoint = params.endpoint;
    const webActionName = 'kafkaFeedWeb';

    var massagedParams = common.massageParamsForWeb(params);
    massagedParams.triggerName = common.getTriggerFQN(params.triggerName);

    if (params.lifecycleEvent === 'CREATE') {
        return common.createTrigger(endpoint, massagedParams, webActionName);
    } else if (params.lifecycleEvent === 'READ') {
        return common.getTrigger(endpoint, massagedParams, webActionName);
    } else if (params.lifecycleEvent === 'UPDATE') {
        return common.updateTrigger(endpoint, massagedParams, webActionName);
    } else if (params.lifecycleEvent === 'DELETE') {
        return common.deleteTrigger(endpoint, massagedParams, webActionName);
    }

    return {
        error: 'unsupported lifecycleEvent'
    };
}

exports.main = main;
