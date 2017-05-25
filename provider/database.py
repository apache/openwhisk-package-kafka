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

from cloudant.client import CouchDB
from cloudant.result import Result

class Database:
    db_prefix = os.getenv('DB_PREFIX', '')
    dbname = db_prefix + 'ow_kafka_triggers'

    username = os.environ['DB_USER']
    password = os.environ['DB_PASS']
    url = os.environ['DB_URL']

    client = CouchDB(username, password, url=url)
    client.connect()

    filters_design_doc_id = '_design/filters'
    only_triggers_view_id = 'only-triggers'

    if dbname in client.all_dbs():
        logging.info('Database exists - connecting to it.')
        database = client[dbname]
    else:
        logging.warn('Database does not exist - creating it.')
        database = client.create_database(dbname)


    def disableTrigger(self, triggerFQN, status_code):
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


    def changesFeed(self):
        return self.database.infinite_changes(include_docs=True)


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
