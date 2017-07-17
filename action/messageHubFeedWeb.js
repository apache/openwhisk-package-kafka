const common = require('./lib/common');
const Database = require('./lib/Database');

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

        if (params.__ow_method === "put") {
            var validatedParams;
            return validateParameters(params)
                .then(cleanParams => {
                    validatedParams = cleanParams;

                    console.log(`VALIDATED: ${JSON.stringify(validatedParams, null, 2)}`);
                    db = new Database(params.DB_URL, params.DB_NAME);

                    // do these in parallel!
                    return Promise.all([
                        db.ensureTriggerIsUnique(validatedParams.triggerName),
                        common.verifyTriggerAuth(validatedParams.triggerURL),
                        checkMessageHubCredentials(validatedParams)
                    ]);
                })
                .then(() => db.recordTrigger(validatedParams))
                .then(() => {
                    console.log('successfully wrote the trigger');
                    resolve({
                        statusCode: 200,
                        headers: {'Content-Type': 'text/plain'},
                        body: validatedParams.uuid
                    });
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

                    resolve({
                        statusCode: statusCode,
                        headers: {'Content-Type': 'text/plain'},
                        body: body
                    });
                });
        } else if (params.__ow_method === "delete") {
            const triggerURL = common.getTriggerURL(params.authKey, params.endpoint, params.triggerName);

            return common.verifyTriggerAuth(triggerURL)
                .then(() => {
                    db = new Database(params.DB_URL, params.DB_NAME);
                    return db.deleteTrigger(params.triggerName);
                })
                .then(() => {
                    console.log('successfully deleted the trigger');
                    resolve({
                        statusCode: 200,
                        headers: {'Content-Type': 'text/plain'},
                        body: "deleted trigger"
                    });
                })
                .catch(reject);
        } else {
            resolve({
                statusCode: 400,
                headers: {'Content-Type': 'text/plain'},
                body: 'unsupported lifecycleEvent'
            });
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
        }, function (authError) {
            console.log(`authError: ${JSON.stringify(authError)}`);
            return Promise.reject( 'Could not authenticate with Message Hub. Please check your credentials.' );
        });
}

exports.main = main;
