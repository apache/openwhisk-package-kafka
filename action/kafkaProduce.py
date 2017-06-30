"""Kafka message producer.

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
"""

import base64
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable


def main(params):
    validationResult = validateParams(params)
    if validationResult[0] is not True:
        return {'error': validationResult[1]}
    else:
        validatedParams = validationResult[1]

    brokers = params['brokers']

    try:
        producer = KafkaProducer(
            api_version_auto_timeout_ms=15000,
            bootstrap_servers=brokers)

        print("Created producer")

        # only use the key parameter if it is present
        if 'key' in validatedParams:
            messageKey = validatedParams['key']
            producer.send(validatedParams['topic'], bytes(validatedParams['value'], 'utf-8'), key=bytes(messageKey, 'utf-8'))
        else:
            producer.send(validatedParams['topic'], bytes(validatedParams['value'], 'utf-8'))

        producer.flush()

        print("Sent message")
    except NoBrokersAvailable:
        # this exception's message is a little too generic
        return {'error': 'No brokers available. Check that your supplied brokers are correct and available.'}
    except Exception as e:
        return {'error': '{}'.format(e)}

    return {"success": True}


def validateParams(params):
    validatedParams = params.copy()
    requiredParams = ['brokers', 'topic', 'value']
    missingParams = []

    for requiredParam in requiredParams:
        if requiredParam not in params:
            missingParams.append(requiredParam)

    if len(missingParams) > 0:
        return (False, "You must supply all of the following parameters: {}".format(', '.join(missingParams)))

    if 'base64DecodeValue' in params and params['base64DecodeValue'] is True:
        try:
            validatedParams['value'] = base64.b64decode(params['value']).decode('utf-8')
        except:
            return (False, "value parameter is not Base64 encoded")

        if len(validatedParams['value']) == 0:
            return (False, "value parameter is not Base64 encoded")

    if 'base64DecodeKey' in params and params['base64DecodeKey'] is True:
        try:
            validatedParams['key'] = base64.b64decode(params['key']).decode('utf-8')
        except:
            return (False, "key parameter is not Base64 encoded")

        if len(validatedParams['key']) == 0:
            return (False, "key parameter is not Base64 encoded")

    return (True, validatedParams)
