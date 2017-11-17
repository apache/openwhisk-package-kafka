const common = require('./lib/common');
const Database = require('./lib/Database');
var moment = require('moment');

/**
 *   Feed to listen to Kafka messages
 *  @param {string} brokers - array of Kafka brokers
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
                        common.verifyTriggerAuth(validatedParams.triggerURL)
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
            const triggerURL = common.getTriggerURL(params.authKey, params.endpoint, params.triggerName);

            return common.verifyTriggerAuth(triggerURL)
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
                            brokers: triggerDoc.brokers
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
            const triggerURL = common.getTriggerURL(params.authKey, params.endpoint, params.triggerName);
            
            return common.verifyTriggerAuth(triggerURL)
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
                    console.log(`Failed to update trigger ${error}`);
                    var statusCode = 500;
                    var body = error.toString();

                    if (error.validationError) {
                        statusCode = 400;
                        body = error.validationError;
                    }
                    resolve(common.webResponse(statusCode, body));
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
        }

        // brokers
        if (rawParams.brokers) {
            validatedParams.brokers = common.validateBrokerParam(rawParams.brokers);
            if (!validatedParams.brokers) {
                reject( { validationError: "You must supply a 'brokers' parameter as an array of Message Hub brokers." });
                return;
            }
        } else {
            reject( { validationError: "You must supply a 'brokers' parameter." });
            return;
        }

        validatedParams.isMessageHub = false;

        resolve(validatedParams);
    });

    return promise;
}

exports.main = main;
