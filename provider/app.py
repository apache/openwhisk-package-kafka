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
import sys
from datetime import datetime
from flask import Flask, jsonify, request
from consumer import Consumer
from consumercollection import ConsumerCollection
from database import Database
from threading import Lock
from thedoctor import TheDoctor
from urlparse import urlparse
from health import generateHealthReport
import requests
from gevent.wsgi import WSGIServer
import urllib


app = Flask(__name__)
app.debug = False
database = Database()

consumers = ConsumerCollection()


@app.route('/triggers/<namespace>/<trigger>', methods=['PUT'])
def postTrigger(namespace, trigger):
    body = request.get_json(force=True, silent=True)
    triggerFQN = '/' + namespace + '/' + trigger
    expectedRoute = '/namespaces/' + urllib.quote(namespace) + '/triggers/' + urllib.quote(trigger)

    if consumers.hasConsumerForTrigger(triggerFQN):
        logging.warn("[{}] Trigger already exists".format(triggerFQN))
        response = jsonify({
            'success': False,
            'error': "trigger already exists"
        })
        response.status_code = 409
    elif not body["triggerURL"].endswith(expectedRoute):
        logging.warn("[{}] Trigger and namespace from route must correspond to triggerURL".format(triggerFQN))
        response = jsonify({
            'success': False,
            'error': "trigger and namespace from route must correspond to triggerURL"
        })
        response.status_code = 409
    else:
        logging.info("[{}] Ensuring user has access rights to post a trigger".format(triggerFQN))
        trigger_get_response = requests.get(body["triggerURL"])
        trigger_get_status_code = trigger_get_response.status_code
        logging.info("[{}] Repsonse status code from trigger authorization {}".format(triggerFQN,
                                                                                      trigger_get_status_code))

        if trigger_get_status_code == 200:
            logging.info("[{}] User authenticated. About to create consumer {}".format(triggerFQN, str(body)))
            createAndRunConsumer(triggerFQN, body)
            logging.info("[{}] Finished creating consumer.".format(triggerFQN))
            response = jsonify({'success': True})
            response.status_code = trigger_get_status_code
        elif trigger_get_status_code == 401:
            logging.warn("[{}] User not authorized to post trigger".format(triggerFQN))
            response = jsonify({
                'success': False,
                'error': 'not authorized'
            })
            response.status_code = trigger_get_status_code
        else:
            logging.warn("[{}] Trigger authentication request failed with error code {}".format(triggerFQN,
                trigger_get_status_code))
            response = jsonify({'success': False})
            response.status_code = trigger_get_status_code

    return response


@app.route('/triggers/<namespace>/<trigger>', methods=['DELETE'])
def deleteTrigger(namespace, trigger):
    auth = request.authorization
    body = request.get_json(force=True, silent=True)

    triggerFQN = '/' + namespace + '/' + trigger
    consumer = consumers.getConsumerForTrigger(triggerFQN)
    if consumer != None:
        if authorizedForTrigger(auth, consumer):
            consumer.shutdown()
            consumers.removeConsumerForTrigger(triggerFQN)
            response = jsonify({'success': True})
        else:
            response = jsonify({'error': 'not authorized'})
            response.status_code = 401
    else:
        response = jsonify({'error': 'not found'})
        response.status_code = 404

    return response


@app.route('/')
def testRoute():
    return jsonify('Hi!')

#TODO call TheDoctor.isAlive() and report on that
@app.route('/health')
def healthRoute():
    return jsonify(generateHealthReport(consumers))


def authorizedForTrigger(auth, consumer):
    triggerURL = urlparse(consumer.triggerURL)

    return (auth != None and consumer != None and triggerURL.username == auth.username and triggerURL.password == auth.password)


def createAndRunConsumer(triggerFQN, params, record=True):
    if app.config['TESTING'] == True:
        logging.debug("Just testing")
    else:
        consumer = Consumer(triggerFQN, params)
        consumer.start()
        consumers.addConsumerForTrigger(triggerFQN, consumer)

        if record:
            database.recordTrigger(triggerFQN, params)


def restoreTriggers():
    for triggerDoc in database.triggers():
        triggerFQN = triggerDoc['_id']
        logging.debug('Restoring trigger {}'.format(triggerFQN))
        createAndRunConsumer(triggerFQN, triggerDoc, record=False)


def main():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # Make sure we log to the console
    streamHandler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s [%(levelname)s]: %(message)s')
    streamHandler.setFormatter(formatter)
    logger.addHandler(streamHandler)

    TheDoctor(consumers).start()

    restoreTriggers()

    port = int(os.getenv('PORT', 5000))
    server = WSGIServer(('', port), app, log=logging.getLogger())
    server.serve_forever()

if __name__ == '__main__':
    main()
