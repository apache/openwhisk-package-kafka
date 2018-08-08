"""Database class.

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
import time

from cloudant.client import CouchDB
from cloudant.client import CouchDatabase
from cloudant.result import Result
from datetime import datetime


class Database:
    db_prefix = os.getenv('DB_PREFIX', '')
    dbname = db_prefix + 'ow_kafka_triggers'

    username = os.environ['DB_USER']
    password = os.environ['DB_PASS']
    url = os.environ['DB_URL']

    filters_design_doc_id = '_design/filters'
    only_triggers_view_id = 'only-triggers'
    by_worker_view_id = 'by-worker'

    instance = os.getenv('INSTANCE', 'messageHubTrigger-0')
    canaryId = "canary-{}".format(instance)

    def __init__(self, timeout=None):
        self.client = CouchDB(self.username, self.password, url=self.url, timeout=timeout, auto_renew=True)
        self.client.connect()

        self.database = CouchDatabase(self.client, self.dbname)

        if self.database.exists():
            logging.info('Database exists - connecting to it.')
        else:
            logging.warn('Database does not exist - creating it.')
            self.database.create()

    def destroy(self):
        if self.client is not None:
            self.client.disconnect()
            self.client = None

    def disableTrigger(self, triggerFQN, status_code, message='Automatically disabled after receiving a {} status code when firing the trigger.'):
        try:
            document = self.database[triggerFQN]

            if document.exists():
                logging.info('Found trigger to disable from DB: {}'.format(triggerFQN))

                status = {
                    'active': False,
                    'dateChanged': long(time.time() * 1000),
                    'reason': {
                        'kind': 'AUTO',
                        'statusCode': status_code,
                        'message': message.format(status_code)
                    }
                }

                document['status'] = status
                document.save()

                logging.info('{} Successfully recorded trigger as disabled.'.format(triggerFQN))
        except Exception as e:
            logging.error('[{}] Uncaught exception while disabling trigger: {}'.format(triggerFQN, e))

    def changesFeed(self, timeout, since=None):
        if since == None:
            return self.database.infinite_changes(include_docs=True, heartbeat=(timeout*1000))
        else:
            return self.database.infinite_changes(include_docs=True, heartbeat=(timeout*1000), since=since)

    def createCanary(self):
        maxRetries = 3
        retryCount = 0

        while retryCount < maxRetries:
            try:
                if self.canaryId in self.database.keys(remote=True):
                    # update the timestamp to cause a document change
                    logging.debug("[database] Canary doc exists, updating it.")

                    myCanaryDocument = self.database[self.canaryId]
                    myCanaryDocument["canary-timestamp"] = datetime.now().isoformat()
                    myCanaryDocument.save()

                    return
                else:
                    # create the canary doc for this instance
                    logging.debug("[database] Canary doc does not exist, creating it.")

                    document = dict()
                    document['_id'] = self.canaryId
                    document['canary-timestamp'] = datetime.now().isoformat()

                    result = self.database.create_document(document)
                    logging.debug('[canary] Successfully wrote canary to DB')

                    return
            except Exception as e:
                retryCount += 1
                logging.error(
                    '[canary] Uncaught exception while writing canary document: {}'.format(e))

        logging.error('[canary] Retried and failed {} times to create a canary'.format(maxRetries))

    def migrate(self):
        logging.info('Starting DB migration')

        by_worker_view = {
            'map': """function(doc) {
                        if(doc.triggerURL && (!doc.status || doc.status.active)) {
                            emit(doc.worker || 'worker0', 1);
                        }
                    }""",
            'reduce': '_count'
        }

        filtersDesignDoc = self.database.get_design_document(self.filters_design_doc_id)

        if filtersDesignDoc.exists():
            if self.by_worker_view_id not in filtersDesignDoc["views"]:
                filtersDesignDoc["views"][self.by_worker_view_id] = by_worker_view
                logging.info('Updating the design doc')
                filtersDesignDoc.save()
        else:
            logging.info('Creating the design doc')

            self.database.create_document({
                '_id': self.filters_design_doc_id,
                'views': {
                    self.only_triggers_view_id: {
                        'map': """function (doc) {
                                    if(doc.triggerURL) {
                                        emit(doc._id, 1);
                                    }
                                }"""
                    },
                    self.by_worker_view_id: by_worker_view
                }
            })

        logging.info('Database migration complete')
