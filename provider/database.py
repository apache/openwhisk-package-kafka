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
import time
import uuid

from cloudant import Cloudant
from cloudant.result import Result
from threading import Lock

class Database:
    db_prefix = os.getenv('DB_PREFIX', '')
    dbname = db_prefix + 'ow_kafka_triggers'
    username = os.environ['CLOUDANT_USER']
    password = os.environ['CLOUDANT_PASS']

    lock = Lock()
    client = Cloudant(username, password, account=username)
    client.connect()

    filters_design_doc_id = '_design/filters'
    only_triggers_view_id = 'only-triggers'

    if dbname in client.all_dbs():
        logging.info('Database exists - connecting to it.')
        database = client[dbname]
    else:
        logging.warn('Database does not exist - creating it.')
        database = client.create_database(dbname)

    def recordTrigger(self, triggerFQN, doc):
        with self.lock:
            try:
                document = dict(doc)
                document['_id'] = triggerFQN

                # set the status as active
                status = {
                    'active': True,
                    'dateChanged': time.time()
                }

                document['status'] = status

                logging.info('Writing trigger {} to DB'.format(triggerFQN))
                result = self.database.create_document(document)
                logging.info('Successfully wrote trigger {} to DB'.format(triggerFQN))

                return result
            except Exception as e:
                logging.error('[{}] Uncaught exception while recording trigger to database: {}'.format(triggerFQN, e))


    def deleteTrigger(self, triggerFQN):
        with self.lock:
            try:
                document = self.database[triggerFQN]

                if document.exists():
                    logging.info('Found trigger to delete from DB: {}'.format(triggerFQN))
                    document.delete()
                    logging.info('Successfully deleted trigger from DB: {}'.format(triggerFQN))
                else:
                    logging.warn('Attempted to delete non-existent trigger from DB: {}'.format(triggerFQN))
            except Exception as e:
                logging.error('[{}] Uncaught exception while deleting trigger from database: {}'.format(triggerFQN, e))

    def disableTrigger(self, triggerFQN, status_code):
        with self.lock:
            try:
                document = self.database[triggerFQN]

                if document.exists():
                    logging.info('Found trigger to disable from DB: {}'.format(triggerFQN))

                    status = {
                        'active': False,
                        'dateChanged': time.time(),
                        'reason': {
                            'kind': 'AUTO',
                            'statusCode': status_code,
                            'message': 'Automatically disabled after receiving a {} status code when firing the trigger.'.format(status_code)
                        }
                    }

                    document['status'] = status
                    document.save()

                    logging.info('{} Successfully recorded trigger as disabled.'.format(triggerFQN))
            except Exception as e:
                logging.error('[{}] Uncaught exception while disabling trigger: {}'.format(triggerFQN, e))


    def triggers(self):
        allDocs = []

        logging.info('Fetching all triggers from DB')
        for document in self.database.get_view_result(self.filters_design_doc_id, self.only_triggers_view_id, include_docs=True):
            allDocs.append(document['doc'])

        logging.info('Successfully retrieved {} triggers'.format(len(allDocs)))
        return allDocs

    def migrate(self):
        logging.info('Starting DB migration')

        filtersDesignDoc = self.database.get_design_document(self.filters_design_doc_id)

        if not filtersDesignDoc.exists():
            logging.info('Creating the design doc')

            # create only-triggers view
            self.database.create_document({
                '_id': self.filters_design_doc_id,
                'views': {
                    self.only_triggers_view_id: {
                        'map': """function (doc) {
                                    if(doc.triggerURL) {
                                        emit(doc._id, 1);
                                    }
                                }"""
                    }
                }
            });
        else:
            logging.info("design doc already exists")

        logging.info('Database migration complete')
