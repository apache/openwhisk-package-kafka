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
                body.triggerURL = 'https://' + whisk.getAuthKey() + "@" + massagedAPIHost + '/api/v1/namespaces/' + namespace + '/triggers/' + trigger;

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

            return validateParameters(params)
                .then(checkMessageHubCredentials)
                .then(putTrigger)
                .then(resolve)
                .catch(reject);
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

            doRequest(options)
                .then(resolve)
                .catch(reject);
        }
    });

    return promise;
}

function checkMessageHubCredentials(params) {
    // make sure we have valid MH credentials
    console.log("Checking Message Hub credentials...");

    // listing topics seems to be the simplest way to check auth
    var topicURL = params.kafka_admin_url + '/admin/topics';

    var options = {
        method: 'GET',
        url: topicURL,
        headers: {
            'X-Auth-Token': params.username + params.password
        }
    };

    return doRequest(options)
        .then(function(body) {
            console.log("Successfully authenticated with Message Hub");

            var topicNames = body.response.map(function(topic) {
                return topic.name
            });

            if(topicNames.indexOf(params.topic) < 0) {
                return Promise.reject('Topic does not exist. You must create the topic first: ' + params.topic);
            } else {
                console.log("Topic exists.");
                // return the params so they can be chained into the next function
                return params;
            }
        }, function(authError) {
            return Promise.reject('Could not authenticate with Message Hub. Please check your credentials.');
        });
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
                    console.log("Response: " + JSON.stringify(body));
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

        validatedParams.isMessageHub = true;
        validatedParams.isJSONData = (typeof rawParams.isJSONData !== 'undefined' && rawParams.isJSONData && (rawParams.isJSONData === true || rawParams.isJSONData.toString().trim().toLowerCase() === 'true'));

        // topic
        if (rawParams.topic && rawParams.topic.length > 0) {
            validatedParams.topic = rawParams.topic;
        } else {
            reject('You must supply a "topic" parameter.');
            return;
        }

        // kafka_brokers_sasl
        if (rawParams.kafka_brokers_sasl) {
            validatedParams.brokers = validateBrokerParam(rawParams.kafka_brokers_sasl);
            if(!validatedParams.brokers) {
                reject('You must supply a "kafka_brokers_sasl" parameter as an array of Message Hub brokers.');
                return;
            }
        } else {
            reject('You must supply a "kafka_brokers_sasl" parameter as an array of Message Hub brokers.');
            return;
        }

        // user
        if (rawParams.user) {
            validatedParams.username = rawParams.user;
        } else {
            reject('You must supply a "user" parameter to authenticate with Message Hub.');
            return;
        }

        // password
        if (rawParams.password) {
            validatedParams.password = rawParams.password;
        } else {
            reject('You must supply a "password" parameter to authenticate with Message Hub.');
            return;
        }

        // kafka_admin_url
        if(rawParams.kafka_admin_url) {
            validatedParams.kafka_admin_url = rawParams.kafka_admin_url;
        } else {
            reject('You must supply a "kafka_admin_url" parameter.');
            return;
        }

        resolve(validatedParams);
    });

    return promise;
}

function validateBrokerParam(brokerParam) {
    if(isNonEmptyArray(brokerParam)) {
        return brokerParam;
    } else if (typeof brokerParam === 'string') {
        return brokerParam.split(',');
    } else {
        return undefined;
    }
}

function isNonEmptyArray(obj) {
    return obj && Array.isArray(obj) && obj.length !== 0;
}
