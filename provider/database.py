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

                logging.info('Writing trigger {} to DB'.format(triggerFQN))
                result = self.database.create_document(document)
                logging.info("Successfully wrote trigger "
                             "{} to DB".format(triggerFQN))

                return result
            except Exception as e:
                logging.error("[{}] Uncaught exception while recording "
                              "trigger to database: {}".format(triggerFQN, e))

    def deleteTrigger(self, triggerFQN):
        with self.lock:
            try:
                document = self.database[triggerFQN]

                if document.exists():
                    logging.info("Found trigger to delete from "
                                 "DB: {}".format(triggerFQN))
                    document.delete()
                    logging.info("Successfully deleted trigger from "
                                 "DB: {}".format(triggerFQN))
                else:
                    logging.warn("Attempted to delete non-existent trigger "
                                 "from DB: {}".format(triggerFQN))
            except Exception as e:
                logging.error("[{}] Uncaught exception while deleting "
                              "trigger from database: {}".format(triggerFQN,
                                                                 e))

    def triggers(self):
        allDocs = []

        logging.info("Fetching all documents from DB")
        for document in Result(self.database.all_docs, include_docs=True):
            allDocs.append(document['doc'])

        logging.info("Successfully retrieved {} "
                     "documents".format(len(allDocs)))
        return allDocs

    def migrate(self):
        logging.info('Starting DB migration')

        for trigger in Result(self.database.all_docs, include_docs=True):
            if 'uuid' not in trigger['doc']:
                logging.info("[{}] Does not have a UUID. Generating "
                             "one...".format(trigger['id']))

                # this little dance seems odd to me. trigger does not
                # have a .save() method, so I am left to fetch the
                # document this way:
                doc = self.database[trigger['id']]
                doc['uuid'] = str(uuid.uuid4())
                doc.save()

                logging.info("[{}] Now has UUID {}".format(trigger['id'],
                                                           doc['uuid']))
            else:
                logging.debug("[{}] Already has UUID".format(trigger['id']))

        logging.info("Database migration complete")
