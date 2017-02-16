var request = require('request');

/**
 *   Feed to listen to Kafka messages
 *  @param {string} brokers - array of Kafka brokers
 *  @param {string} topic - topic to subscribe to
 *  @param {bool}   isJSONData - attempt to parse messages as JSON
 *  @param {bool}   isBinaryKey - encode message as Base64
 *  @param {bool}   isBinaryValue - encode key as Base64
 *  @param {string} endpoint - address to OpenWhisk deployment
 */
function main(params) {
    var promise = new Promise(function(resolve, reject) {
        if(!params.package_endpoint) {
            reject('Could not find the package_endpoint parameter.');
            return;
        }

        var triggerComponents = params.triggerName.split("/");
        var namespace = encodeURIComponent(process.env['__OW_NAMESPACE']);
        var trigger = encodeURIComponent(triggerComponents[2]);
        var feedServiceURL = 'http://' + params.package_endpoint + '/triggers/' + namespace + '/' + trigger;

        if (params.lifecycleEvent === 'CREATE') {
            // makes a PUT request to create the trigger in the feed service provider
            var putTrigger = function(validatedParams) {
                var body = validatedParams;

                // params.endpoint may already include the protocol - if so,
                // strip it out
                var massagedAPIHost = params.endpoint.replace(/https?:\/\/(.*)/, "$1");
                body.triggerURL = 'https://' + process.env['__OW_API_KEY'] + "@" + massagedAPIHost + '/api/v1/namespaces/' + namespace + '/triggers/' + trigger;

                var options = {
                    method: 'PUT',
                    url: feedServiceURL,
                    body: JSON.stringify(body),
                    headers: {
                        'Content-Type': 'application/json',
                        'User-Agent': 'whisk'
                    }
                };

                return doRequest(options);
            };

            validateParameters(params)
                .then(putTrigger)
                .then(resolve)
                .catch(reject);
        } else if (params.lifecycleEvent === 'DELETE') {
            var authorizationHeader = 'Basic ' + new Buffer(process.env['__OW_API_KEY']).toString('base64');

            var options = {
                method: 'DELETE',
                url: feedServiceURL,
                headers: {
                    'Content-Type': 'application/json',
                    'Authorization': authorizationHeader,
                    'User-Agent': 'whisk'
                }
            };

            doRequest(options)
                .then(resolve)
                .catch(reject);
        }
    });

    return promise;
}

function doRequest(options) {
    var requestPromise = new Promise(function (resolve, reject) {
        request(options, function (error, response, body) {
            if (error) {
                reject({
                    response: response,
                    error: error,
                    body: JSON.parse(body)
                });
            } else {
                console.log("Status code: " + response.statusCode);

                if (response.statusCode >= 400) {
                    console.log("Response from Kafka feed service: " + body);
                    reject({
                        statusCode: response.statusCode,
                        response: JSON.parse(body)
                    });
                } else {
                    resolve({
                        response: JSON.parse(body)
                    });
                }
            }
        });
    });

    return requestPromise;
}

function validateParameters(rawParams) {
    var promise = new Promise(function(resolve, reject) {
        var validatedParams = {};

        validatedParams.isMessageHub = false;
        validatedParams.isJSONData = (typeof rawParams.isJSONData !== 'undefined' && rawParams.isJSONData && (rawParams.isJSONData === true || rawParams.isJSONData.toString().trim().toLowerCase() === 'true'));
        validatedParams.isBinaryValue = (typeof rawParams.isBinaryValue !== 'undefined' && rawParams.isBinaryValue && (rawParams.isBinaryValue === true || rawParams.isBinaryValue.toString().trim().toLowerCase() === 'true'));
        validatedParams.isBinaryKey = (typeof rawParams.isBinaryKey !== 'undefined' && rawParams.isBinaryKey && (rawParams.isBinaryKey === true || rawParams.isBinaryKey.toString().trim().toLowerCase() === 'true'));

        if (validatedParams.isJSONData && validatedParams.isBinaryValue) {
            reject('isJSONData and isBinaryValue cannot both be enabled.');
            return;
        }

        if (rawParams.topic && rawParams.topic.length > 0) {
            validatedParams.topic = rawParams.topic;
        } else {
            reject('You must supply a "topic" parameter.');
            return;
        }

        if (isNonEmptyArray(rawParams.brokers)) {
            validatedParams.brokers = rawParams.brokers;
        } else {
            reject('You must supply a "brokers" parameter as an array of Kafka brokers.');
            return;
        }

        resolve(validatedParams);
    });

    return promise;
}

function isNonEmptyArray(obj) {
    return obj && Array.isArray(obj) && obj.length !== 0;
}
