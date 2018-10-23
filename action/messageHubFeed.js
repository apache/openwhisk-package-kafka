const common = require('./lib/common');

/**
 *   Feed to listen to MessageHub messages
 *  @param {string} kafka_brokers_sasl - array of Message Hub brokers
 *  @param {string} username - Kafka username
 *  @param {string} password - Kafka password
 *  @param {string} topic - topic to subscribe to
 *  @param {bool}   isJSONData - attempt to parse messages as JSON
 *  @param {bool}   isBinaryKey - encode key as Base64
 *  @param {bool}   isBinaryValue - encode message as Base64
 *  @param {string} endpoint - address to OpenWhisk deployment (expected to be bound at deployment)
 */
function main(params) {
    const endpoint = params.endpoint;
    const webActionName = 'messageHubFeedWeb'

    var massagedParams = common.massageParamsForWeb(params);
    massagedParams.triggerName = common.getTriggerFQN(params.triggerName);

    var iamKey = process.env.__OW_IAM_NAMESPACE_API_KEY;
    massagedParams.authKey = iamKey || process.env.__OW_API_KEY;
    massagedParams.isIamKey = iamKey !== undefined;

    if (massagedParams.isIamKey) {
        massagedParams.iamUrl = process.env.__OW_IAM_API_URL;
        massagedParams.namespaceCRN = process.env.__OW_NAMESPACE_CRN;
    }

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
