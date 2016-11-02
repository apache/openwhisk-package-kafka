var request = require('request');

/**
 *   Feed to listen to MessageHub messages
 *  @param {string} kafka_brokers_sasl - array of Message Hub brokers
 *  @param {string} username - Kafka username
 *  @param {string} password - Kafka password
 *  @param {string} topic - topic to subscribe to
 *  @param {bool}   isJSONData - attempt to parse messages as JSON
 *  @param {string} endpoint - address to OpenWhisk deployment
 */
function main(params) {
    if(!params.package_endpoint) {
        whisk.error('Could not find the package_endpoint parameter.');
        return;
    }

    var triggerComponents = params.triggerName.split("/");
    var namespace = encodeURIComponent(triggerComponents[1]);
    var trigger = encodeURIComponent(triggerComponents[2]);

    var feedServiceURL = 'http://' + params.package_endpoint + '/triggers/' + namespace + '/' + trigger;

    if (params.lifecycleEvent === 'CREATE') {
        var validatedParams = validateParameters(params);
        if (!validatedParams) {
            // whisk.error has already been called.
            // all that remains is to bail out.
            return;
        }

        var body = validatedParams;
        body.triggerURL = 'https://' + whisk.getAuthKey() + "@" + params.endpoint + '/api/v1/namespaces/' + namespace + '/triggers/' + trigger;

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
    } else if (params.lifecycleEvent === 'DELETE') {
        var authorizationHeader = 'Basic ' + new Buffer(whisk.getAuthKey()).toString('base64');

        var options = {
            method: 'DELETE',
            url: feedServiceURL,
            headers: {
                'Content-Type': 'application/json',
                'Authorization': authorizationHeader,
                'User-Agent': 'whisk'
            }
        };

        return doRequest(options)
    }
}

function doRequest(options) {
    var promise = new Promise(function (resolve, reject) {
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
                    console.log("Response from Message Hub feed service: " + body);
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

    return promise;
}

function validateParameters(rawParams) {
    var validatedParams = {};

    // default value for isJSONData is false
    validatedParams.isJSONData = rawParams.isJSONData ? rawParams.isJSONData : false;

    if (rawParams.topic && rawParams.topic.length > 0) {
        validatedParams.topic = rawParams.topic;
    } else {
        whisk.error('You must supply a "topic" parameter.');
        return;
    }

    if (isNonEmptyArray(rawParams.kafka_brokers_sasl)) {
        validatedParams.isMessageHub = true;
        validatedParams.brokers = rawParams.kafka_brokers_sasl;

        if (rawParams.user) {
            validatedParams.username = rawParams.user
        } else {
            whisk.error('You must supply a "user" parameter to authenticate with Message Hub.');
            return;
        }

        if (rawParams.password) {
            validatedParams.password = rawParams.password;
        } else {
            whisk.error('You must supply a "password" parameter to authenticate with Message Hub.');
            return;
        }
    } else {
        whisk.error('You must supply a "kafka_brokers_sasl" parameter as an array of Message Hub brokers.');
        return;
    }

    return validatedParams;
}

function isNonEmptyArray(obj) {
    return obj && Array.isArray(obj) && obj.length !== 0;
}
