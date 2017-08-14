const common = require('./lib/common');
const Database = require('./lib/Database');

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
                        common.verifyTriggerAuth(validatedParams.triggerURL)
                    ]);
                })
                .then(() => db.recordTrigger(validatedParams))
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
