# Copyright 2016 IBM Corp. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import os
from datetime import datetime
from flask import Flask, jsonify, request
from consumer import Consumer
from database import Database
from urlparse import urlparse

app = Flask(__name__)
database = Database()

consumers = dict()


@app.route('/triggers/<namespace>/<trigger>', methods=['PUT'])
def postTrigger(namespace, trigger):
    body = request.get_json(force=True, silent=True)

    triggerFQN = '/' + namespace + '/' + trigger
    logging.info("About to create consumer with " +
                 triggerFQN + " " + str(body))
    createAndRunConsumer(triggerFQN, body)
    logging.info("Finished creating consumer.")
    result = {'success': True}
    return jsonify(result)


@app.route('/triggers/<namespace>/<trigger>', methods=['DELETE'])
def deleteTrigger(namespace, trigger):
    auth = request.authorization
    body = request.get_json(force=True, silent=True)

    triggerFQN = '/' + namespace + '/' + trigger
    consumer = consumers.get(triggerFQN)
    if consumer != None:
        if authorizedForTrigger(auth, consumer):
            consumer.shutdown()
            del consumers[triggerFQN]
            response = jsonify({'success': True})
        else:
            response = jsonify({'error': 'not authorized'})
            response.status_code = 401
    else:
        response = jsonify({'error': 'not found'})
        response.status_code = 404

    return response


@app.route('/ping')
def testRoute():
    return jsonify('pong')


@app.route('/health')
def healthRoute():
    currentTime = datetime.now()
    delta = currentTime - startTime
    uptimeSeconds = int(round(delta.total_seconds()))
    return jsonify({'uptime': uptimeSeconds})


def authorizedForTrigger(auth, consumer):
    triggerURL = urlparse(consumer.triggerURL)

    return (auth != None and consumer != None and triggerURL.username == auth.username and triggerURL.password == auth.password)


def createAndRunConsumer(triggerFQN, params, record=True):
    if app.config['TESTING'] == True:
        logging.debug("Just testing")
    else:
        consumer = Consumer(triggerFQN, params)
        # TODO
        # only record after ensuring sucessful start
        # handle db errors/retries?
        consumer.start()
        consumers[triggerFQN] = consumer

        if record:
            database.recordTrigger(triggerFQN, params)


def restoreTriggers():
    for triggerDoc in database.triggers():
        triggerFQN = triggerDoc['_id']
        logging.info('Restoring trigger {}'.format(triggerFQN))
        createAndRunConsumer(triggerFQN, triggerDoc, record=False)

port = os.getenv('PORT', '5000')
if __name__ == "__main__":
    startTime = datetime.now()
    logging.basicConfig(
        format='%(asctime)s [%(levelname)s]: %(message)s', level=logging.INFO)
    restoreTriggers()
    app.run(host='0.0.0.0', port=int(port))
