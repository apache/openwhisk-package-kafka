const request = require('request-promise');

function triggerComponents(triggerName) {
    var split = triggerName.split("/");

    return {
        namespace: split[1],
        triggerName: split[2]
    };
}

function getTriggerURL(authKey, endpoint, triggerName) {
    var massagedAPIHost = endpoint.replace(/https?:\/\/(.*)/, "$1");

    var components = triggerComponents(triggerName);
    var namespace = components.namespace;
    var trigger = components.triggerName;

    var url = `https://${authKey}@${massagedAPIHost}/api/v1/namespaces/${encodeURIComponent(namespace)}/triggers/${encodeURIComponent(trigger)}`;

    return url;
}

function verifyTriggerAuth(triggerURL) {
    var options = {
        method: 'GET',
        url: triggerURL,
        rejectUnauthorized: false,
        headers: {
            'Content-Type': 'application/json',
            'User-Agent': 'whisk'
        }
    };

    return request(options)
        .catch(err => {
            console.log(`Trigger auth error: ${JSON.stringify(err)}`);
            return Promise.reject({ authError: 'You are not authorized for this trigger.'});
        });
}

function validateBrokerParam(brokerParam) {
    if (isNonEmptyArray(brokerParam)) {
        return brokerParam;
    } else if (typeof brokerParam === 'string') {
        return brokerParam.split(',');
    } else {
        return undefined;
    }
}

function getBooleanFromArgs(args, key) {
    return (typeof args[key] !== 'undefined' && args[key] && (args[key] === true || args[key].toString().trim().toLowerCase() === 'true'));
}

function isNonEmptyArray(obj) {
    return obj && Array.isArray(obj) && obj.length !== 0;
}

// Return the trigger FQN with the resolved namespace
// This is required to avoid naming conflicts when using the default namespace "_"
function getTriggerFQN(triggerName) {
    var components = triggerName.split('/');
    return `/${process.env['__OW_NAMESPACE']}/${components[2]}`;
}

function massageParamsForWeb(rawParams) {
    var massagedParams = Object.assign({}, rawParams);

    // remove these parameters as they may conflict with bound parameters of the web action
    delete massagedParams.endpoint;
    delete massagedParams.bluemixServiceName;
    delete massagedParams.lifecycleEvent;

    return massagedParams;
}

function getWebActionURL(endpoint, actionName) {
    return `https://${endpoint}/api/v1/web/whisk.system/messagingWeb/${actionName}.http`;
}

function createTrigger(endpoint, params, actionName) {
    var options = {
        method: 'POST',
        url: getWebActionURL(endpoint, actionName),
        rejectUnauthorized: false,
        json: true,
        body: params,
        headers: {
            'Content-Type': 'application/json',
            'Accept': 'text/plain',
            'User-Agent': 'whisk'
        }
    };

    return request(options)
        .then(response => {
            console.log(`response ${JSON.stringify(response, null, 2)}`)
            return {
                uuid: response
            };
        })
        .catch(error => {
            console.log(`Error creating trigger: ${JSON.stringify(error, null, 2)}`);
            return Promise.reject(error.response.body);
        });
}

function deleteTrigger(endpoint, params, actionName) {
    var options = {
        method: 'DELETE',
        url: getWebActionURL(endpoint, actionName),
        rejectUnauthorized: false,
        json: true,
        body: params,
        headers: {
            'Content-Type': 'application/json',
            'Accept': 'text/plain',
            'User-Agent': 'whisk'
        }
    };

    return request(options)
        .then(response => {
            // do not pass the response back to the caller, its contents are secret
            return;
        }).catch(error => {
            console.log(`Error deleting trigger: ${JSON.stringify(error, null, 2)}`);
            return Promise.reject(error.response.body);
        });
}

function getTrigger(endpoint, params, actionName) {
    var options = {
        method: 'GET',
        url: getWebActionURL(endpoint, actionName),
        rejectUnauthorized: false,
        json: true,
        qs: params,
        headers: {
            'Accept': 'application/json',
            'User-Agent': 'whisk'
        }
    };

    return request(options)
        .then(response => {
            return response;
        })
        .catch(error => {
            console.log(`Error fetching trigger: ${JSON.stringify(error, null, 2)}`);
            return Promise.reject(error.response.body);
        });
}

function updateTrigger(endpoint, params, actionName) {
    var options = {
        method: 'PUT',
        url: getWebActionURL(endpoint, actionName),
        rejectUnauthorized: false,
        json: true,
        body: params,
        headers: {
            'Content-Type': 'application/json',
            'Accept': 'text/plain',
            'User-Agent': 'whisk'
        }
    };

    return request(options)
        .then(response => {
            return;
        })
        .catch(error => {
            console.log(`Error updating trigger: ${JSON.stringify(error, null, 2)}`);
            return Promise.reject(error.response.body);
        });
}

// perform parameter validation that is common to both feed actions
function performCommonParameterValidation(rawParams) {
    var validatedParams = {};

    // topic
    if (rawParams.topic && rawParams.topic.length > 0) {
        validatedParams.topic = rawParams.topic;
    } else {
        return { validationError: "You must supply a 'topic' parameter." };
    }

    // triggerName
    if (rawParams.triggerName) {
        validatedParams.triggerName = rawParams.triggerName;
    } else {
        return { validationError: "You must supply a 'triggerName' parameter." };
    }

    validatedParams.isJSONData = getBooleanFromArgs(rawParams, 'isJSONData');
    validatedParams.isBinaryValue = getBooleanFromArgs(rawParams, 'isBinaryValue');

    if (validatedParams.isJSONData && validatedParams.isBinaryValue) {
        return { validationError: 'isJSONData and isBinaryValue cannot both be enabled.' };
    }

    // now that everything else is valid, let's add these
    validatedParams.isBinaryKey = getBooleanFromArgs(rawParams, 'isBinaryKey');
    validatedParams.authKey = rawParams.authKey;
    validatedParams.triggerURL = getTriggerURL(validatedParams.authKey, rawParams.endpoint, rawParams.triggerName);

    const uuid = require('uuid');
    validatedParams.uuid = uuid.v4();

    return { validatedParams: validatedParams };
}

function performUpdateParameterValidation(params, doc) {
    return new Promise((resolve, reject) => {

        if (params.isBinaryKey !== undefined || params.isBinaryValue !== undefined || params.isJSONData !== undefined) {
            var updatedParams = {
                isJSONData: doc.isJSONData,
                isBinaryKey: doc.isBinaryKey,
                isBinaryValue: doc.isBinaryValue
            };

            if (params.isJSONData !== undefined) {
                updatedParams.isJSONData = getBooleanFromArgs(params, 'isJSONData');
            }

            if (params.isBinaryValue !== undefined) {
                updatedParams.isBinaryValue = getBooleanFromArgs(params, 'isBinaryValue');
            }

            if (updatedParams.isJSONData && updatedParams.isBinaryValue) {
                reject({ validationError: 'isJSONData and isBinaryValue cannot both be enabled.' });
            }

            if (params.isBinaryKey !== undefined) {
                updatedParams.isBinaryKey = getBooleanFromArgs(params, 'isBinaryKey');
            }
            resolve(updatedParams);
        } else {
            // cannot update any other parameters
            reject({ validationError: 'At least one of isJsonData, isBinaryKey, or isBinaryValue must be supplied.' });
        }
    });
}

function webResponse(code, body, contentType = 'text/plain') {
    return {
        statusCode: code,
        headers: {
            'Content-Type': contentType
        },
        body: body
    };
}

module.exports = {
    'createTrigger': createTrigger,
    'deleteTrigger': deleteTrigger,
    'getTrigger': getTrigger,
    'updateTrigger': updateTrigger,
    'getBooleanFromArgs': getBooleanFromArgs,
    'getTriggerFQN': getTriggerFQN,
    'getTriggerURL': getTriggerURL,
    'massageParamsForWeb': massageParamsForWeb,
    'performCommonParameterValidation': performCommonParameterValidation,
    'performUpdateParameterValidation': performUpdateParameterValidation,
    'validateBrokerParam': validateBrokerParam,
    'verifyTriggerAuth': verifyTriggerAuth,
    'webResponse': webResponse
};
