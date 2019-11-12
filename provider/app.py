"""Flask application.

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

import logging
import os

from flask import Flask, jsonify
from consumercollection import ConsumerCollection
from database import Database
from thedoctor import TheDoctor
from health import generateHealthReport
from gevent.wsgi import WSGIServer
from service import Service


app = Flask(__name__)
app.debug = False

database = None
consumers = ConsumerCollection()
feedService = None


@app.route('/')
def testRoute():
    return jsonify('Hi!')


# TODO call TheDoctor.isAlive() and report on that
@app.route('/health')
def healthRoute():
    return jsonify(generateHealthReport(consumers, feedService.lastCanaryTime))


def main():
    logLevels = {
        "info": logging.INFO,
        "debug": logging.DEBUG,
        "error": logging.ERROR,
        "warning": logging.WARNING,
        "critical": logging.CRITICAL
    }
    logger = logging.getLogger()
    logger.setLevel(logLevels.get(os.getenv('LOG_LEVEL', "info")))

    component = os.getenv('INSTANCE', 'messageHubTrigger-0')

    # Make sure we log to the console
    streamHandler = logging.StreamHandler()
    formatter = logging.Formatter('[%(asctime)s.%(msecs)03dZ] [%(levelname)s] [??] [kafkatriggers] %(message)s', datefmt="%Y-%m-%dT%H:%M:%S")
    streamHandler.setFormatter(formatter)
    logger.addHandler(streamHandler)

    # also log to file if /logs is present
    if os.path.isdir('/logs'):
        fh = logging.FileHandler('/logs/{}_logs.log'.format(component))
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    local_dev = os.getenv('LOCAL_DEV', 'False')
    logging.debug('LOCAL_DEV is {} {}'.format(local_dev, type(local_dev)))
    global check_ssl
    check_ssl = (local_dev == 'False')
    logging.info('check_ssl is {} {}'.format(check_ssl, type(check_ssl)))

    generic_kafka = os.getenv('GENERIC_KAFKA', 'True')
    logging.debug('GENERIC_KAFKA is {} {}'.format(generic_kafka, type(generic_kafka)))
    global enable_generic_kafka
    enable_generic_kafka = (generic_kafka == 'True')
    logging.info('enable_generic_kafka is {} {}'.format(enable_generic_kafka, type(enable_generic_kafka)))

    global database
    database = Database()
    database.migrate()

    TheDoctor(consumers).start()

    global feedService
    feedService = Service(consumers)
    feedService.start()

    port = int(os.getenv('PORT', 5000))
    server = WSGIServer(('', port), app, log=logging.getLogger())
    server.serve_forever()

if __name__ == '__main__':
    main()
