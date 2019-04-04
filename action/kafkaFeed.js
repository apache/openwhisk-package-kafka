/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
