const common = require('./lib/common');
const Database = require('./lib/Database');
const itm = require('@ibm-functions/iam-token-manager');
var moment = require('moment');

/**
 *   Feed to listen to MessageHub messages
 *  @param {string} brokers - array of Message Hub brokers
 *  @param {string} username - Kafka username
 *  @param {string} password - Kafka password
 *  @param {string} topic - topic to subscribe to
 *  @param {bool}   isJSONData - attempt to parse messages as JSON
 *  @param {bool}   isBinaryKey - encode key as Base64
 *  @param {bool}   isBinaryValue - encode message as Base64
 *  @param {string} endpoint - address to OpenWhisk deployment (expected to be bound at deployment)
 *  @param {string} DB_URL - URL for the DB, must include authentication (expected to be bound at deployment)
 *  @param {string} DB_NAME - DB name (expected to be bound at deployment)
 */
function main(params) {
    var promise = new Promise((resolve, reject) => {
        // hold off initializing this until definitely needed
        var db;

        if (params.__ow_method === "post") {
            var validatedParams;
            return validateParameters(params)
                .then(cleanParams => {
                    validatedParams = cleanParams;

                    console.log(`VALIDATED: ${JSON.stringify(validatedParams, null, 2)}`);
                    db = new Database(params.DB_URL, params.DB_NAME);

                    // do these in parallel!
                    return Promise.all([
                        db.ensureTriggerIsUnique(validatedParams.triggerName),
                        verifyTriggerAuth(validatedParams.triggerURL, params.authKey, params.isIamKey, params.iamUrl),
                        checkMessageHubCredentials(validatedParams)
                    ]);
                })
                .then(() => {
                    var workers = (params.workers || []);
                    return db.getTriggerAssignment(workers)
                })
                .then((worker) => {
                    validatedParams['worker'] = worker;
                    return db.recordTrigger(validatedParams);
                })
                .then(() => {
                    console.log('successfully wrote the trigger');
                    resolve(common.webResponse(200, validatedParams.uuid));
                })
                .catch(error => {
                    console.log(`Failed to write the trigger ${error}`);

                    // defaults to potentially be overridden
                    var statusCode = 500;
                    var body = error.toString();

                    if(error.validationError) {
                        statusCode = 400;
                        body = error.validationError;
                    } else if(error.authError) {
                        statusCode = 401;
                        body = error.authError;
                    }

                    resolve(common.webResponse(statusCode, body));
                });
        } else if (params.__ow_method === "get") {
            const triggerURL = common.getTriggerURL(params.endpoint, params.triggerName);

            return verifyTriggerAuth(triggerURL, params.authKey, params.isIamKey, params.iamUrl)
                .then(() => {
                    db = new Database(params.DB_URL, params.DB_NAME);
                    return db.getTrigger(params.triggerName);
                })
                .then((triggerDoc) => {
                    var body = {
                        config: {
                            triggerName: triggerDoc.triggerName,
                            topic: triggerDoc.topic,
                            isJSONData: triggerDoc.isJSONData,
                            isBinaryValue: triggerDoc.isBinaryValue,
                            isBinaryKey: triggerDoc.isBinaryKey,
                            kafka_brokers_sasl: triggerDoc.brokers,
                            kafka_admin_url: triggerDoc.kafka_admin_url,
                            user: triggerDoc.username,
                            password: triggerDoc.password
                        },
                        status: {
                            active: triggerDoc.status.active,
                            dateChanged: moment(triggerDoc.status.dateChanged).utc().valueOf(),
                            dateChangedISO: moment(triggerDoc.status.dateChanged).utc().format(),
                            reason: triggerDoc.status.reason
                        }
                    }
                    resolve(common.webResponse(200, body, 'application/json'));
                })
                .catch(error => {
                    resolve(common.webResponse(500, error.toString()));
                });
        } else if (params.__ow_method === "put") {
            const triggerURL = common.getTriggerURL(params.endpoint, params.triggerName);

            return verifyTriggerAuth(triggerURL, params.authKey, params.isIamKey, params.iamUrl)
                .then(() => {
                    db = new Database(params.DB_URL, params.DB_NAME);
                    return db.getTrigger(params.triggerName);
                })
                .then(triggerDoc => {
                    if (!triggerDoc.status.active) {
                        resolve(common.webResponse(400, `${params.triggerName} cannot be updated because it is disabled`));
                    }
                    return common.performUpdateParameterValidation(params, triggerDoc)
                    .then(updatedParams => db.updateTrigger(triggerDoc, updatedParams))
                })
                .then(() => {
                    console.log('successfully updated the trigger');
                    resolve(common.webResponse(200, 'updated trigger'));
                })
                .catch(error => {
                    console.log(`Failed to update trigger: ${error}`);
                    var statusCode = 500;
                    var body = error.toString();

                    if (error.validationError) {
                        statusCode = 400;
                        body = error.validationError;
                    }
                    resolve(common.webResponse(statusCode, body));
                });
        } else if (params.__ow_method === "delete") {
            const triggerURL = common.getTriggerURL(params.endpoint, params.triggerName);

            return verifyTriggerAuth(triggerURL, params.authKey, params.isIamKey, params.iamUrl)
                .then(() => {
                    db = new Database(params.DB_URL, params.DB_NAME);
                    return db.deleteTrigger(params.triggerName);
                })
                .then(() => {
                    console.log('successfully deleted the trigger');
                    resolve(common.webResponse(200, 'deleted trigger'));
                })
                .catch(error => {
                    console.log(`Failed to remove trigger ${error}`);
                    resolve(common.webResponse(500, error.toString()));
                });
        } else {
            resolve(common.webResponse(400, 'unsupported lifecycleEvent'));
        }
    });

    return promise;
}

function validateParameters(rawParams) {
    var promise = new Promise((resolve, reject) => {
        var validatedParams;

        var commonValidationResult = common.performCommonParameterValidation(rawParams);
        if(commonValidationResult.validationError) {
            reject(commonValidationResult);
            return;
        } else {
            validatedParams = commonValidationResult.validatedParams;

            if (rawParams.isIamKey != undefined) {
                validatedParams.isIamKey = rawParams.isIamKey;
            } else {
                validatedParams.isIamKey = false
            }

            if (rawParams.iamUrl) {
                validatedParams.iamUrl = rawParams.iamUrl;
            }
        }

        // kafka_brokers_sasl
        if (rawParams.kafka_brokers_sasl) {
            validatedParams.brokers = common.validateBrokerParam(rawParams.kafka_brokers_sasl);
            if (!validatedParams.brokers) {
                reject( { validationError: "You must supply a 'kafka_brokers_sasl' parameter as an array of Message Hub brokers." });
                return;
            }
        } else {
            reject( { validationError: "You must supply a 'kafka_brokers_sasl' parameter." });
            return;
        }

        // user
        if (rawParams.user) {
            validatedParams.username = rawParams.user;
        } else {
            reject( { validationError: "You must supply a 'user' parameter to authenticate with Message Hub." });
            return;
        }

        // password
        if (rawParams.password) {
            validatedParams.password = rawParams.password;
        } else {
            reject( { validationError: "You must supply a 'password' parameter to authenticate with Message Hub." });
            return;
        }

        // kafka_admin_url
        if (rawParams.kafka_admin_url) {
            validatedParams.kafka_admin_url = rawParams.kafka_admin_url;
        } else {
            reject( { validationError: "You must supply a 'kafka_admin_url' parameter." });
            return;
        }

        validatedParams.isMessageHub = true;

        resolve(validatedParams);
    });

    return promise;
}

function checkMessageHubCredentials(params) {
    // listing topics seems to be the simplest way to check auth
    var topicURL = params.kafka_admin_url + '/admin/topics';

    var options = {
        method: 'GET',
        url: topicURL,
        json: true,
        headers: {
            'X-Auth-Token': params.username + params.password
        }
    };

    const request = require('request-promise');

    return request(options)
        .then(body => {
            console.log("Successfully authenticated with Message Hub");

            var topicNames = body.map(topic => {
                return topic.name
            });

            if (topicNames.indexOf(params.topic) < 0) {
                return Promise.reject( 'Topic does not exist. You must create the topic first: ' + params.topic );
            }
        }, function (err) {
            console.log(`Error: ${JSON.stringify(err)}`);

            if (err.statusCode === 403) {
                return Promise.reject( 'Could not authenticate with Message Hub. Please check your credentials.' );
            }
        });
}

function verifyTriggerAuth(triggerURL, apiKey, isIamKey, iamUrl) {
    if (isIamKey) {
        return new itm({ 'iamApikey': apiKey, 'iamUrl': iamUrl }).getToken().then( token => common.verifyTriggerAuth(triggerURL, { bearer: token }));
    } else {
        var auth = apiKey.split(':');
        return common.verifyTriggerAuth(triggerURL, { user: auth[0], pass: auth[1] });
    }
}

exports.main = main;
