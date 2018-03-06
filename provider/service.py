"""Service class, CanaryDocumentGenerator class.

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
import requests
import json

from consumer import Consumer
from database import Database
from datetime import datetime
from datetimeutils import secondsSince
from requests.exceptions import ConnectionError, ReadTimeout
from threading import Thread

# How often to produce canary documents
canaryInterval = 60  # seconds

# How long the changes feed should poll before timing out
changesFeedTimeout = 30  # seconds

local_dev = os.getenv('LOCAL_DEV', 'False')
check_ssl = (local_dev == 'False')

class Service (Thread):
    def __init__(self, consumers):
        Thread.__init__(self)
        self.daemon = True

        self.database = None
        self.lastSequence = None
        self.canaryGenerator = CanaryDocumentGenerator()

        self.consumers = consumers
        self.workerId = os.getenv("WORKER", "worker0")

        self.session = requests.Session()

    def run(self):
        self.canaryGenerator.start()

        while True:
            try:
                if self.database is not None:
                    logging.info("Shutting down existing DB client")
                    self.database.destroy()

                logging.info("Starting changes feed")
                self.database = Database(timeout=changesFeedTimeout)
                self.changes = self.database.changesFeed(timeout=changesFeedTimeout, since=self.lastSequence)

                self.lastCanaryTime = datetime.now()

                for change in self.changes:
                    # change could be None because the changes feed will timeout
                    # if it hasn't detected any changes. This timeout allows us to
                    # check whether or not the feed is capable of detecting canary
                    # documents
                    if change != None:
                        # Record the sequence in case the changes feed needs to be
                        # restarted. This way the new feed can pick up right where
                        # the old one left off.
                        self.lastSequence = change['seq']

                        if "deleted" in change and change["deleted"] == True:
                            logging.info('[changes] Found a delete')
                            consumer = self.consumers.getConsumerForTrigger(change['id'])
                            if consumer != None:
                                if consumer.desiredState() == Consumer.State.Disabled:
                                    # just remove it from memory
                                    logging.info('[{}] Removing disabled trigger'.format(consumer.trigger))
                                    self.consumers.removeConsumerForTrigger(consumer.trigger)
                                else:
                                    logging.info('[{}] Shutting down running trigger'.format(consumer.trigger))
                                    consumer.shutdown()
                        # since we can't use a filter function for the feed (then
                        # you don't get deletes) we need to manually verify this
                        # is a valid trigger doc that has changed
                        elif 'triggerURL' in change['doc']:
                            logging.info('[changes] Found a change in a trigger document')
                            document = change['doc']
                            triggerIsAssignedToMe = self.__isTriggerDocAssignedToMe(document)

                            if not self.consumers.hasConsumerForTrigger(change["id"]):
                                if triggerIsAssignedToMe:
                                    if self.__isTriggerDocActive(document):
                                        logging.info('[{}] Found a new trigger to create'.format(change["id"]))
                                        self.createAndRunConsumer(document)
                                else:
                                    logging.info("[{}] Found a new trigger, but is assigned to another worker: {}".format(change["id"], document["worker"]))
                            else:
                                existingConsumer = self.consumers.getConsumerForTrigger(change["id"])

                                if existingConsumer.desiredState() == Consumer.State.Running and not self.__isTriggerDocActive(document):
                                    # running trigger should become disabled
                                    # this should be done regardless of which worker the document claims to be assigned to
                                    logging.info('[{}] Existing running trigger should become disabled'.format(change["id"]))
                                    existingConsumer.shutdown()
                                elif triggerIsAssignedToMe:
                                    logging.info('[{}] Found a change to an existing trigger'.format(change["id"]))

                                    if existingConsumer.desiredState() == Consumer.State.Disabled and self.__isTriggerDocActive(document):
                                        # disabled trigger has become active
                                        logging.info('[{}] Existing disabled trigger should become active'.format(change["id"]))
                                        self.createAndRunConsumer(document)
                                else:
                                    # trigger has become reassigned to a different worker
                                    logging.info("[{}] Shutting down trigger as it has been re-assigned to {}".format(change["id"], document["worker"]))
                                    existingConsumer.shutdown()
                        elif 'canary-timestamp' in change['doc']:
                            # found a canary - update lastCanaryTime
                            logging.info('[canary] I found a canary. The last one was {} seconds ago.'.format(secondsSince(self.lastCanaryTime)))
                            self.lastCanaryTime = datetime.now()
                        else:
                            logging.debug('[changes] Found a change for a non-trigger document')
            except Exception as e:
                logging.error('[canary] Exception caught from changes feed. Restarting changes feed...')
                logging.error(e)
                self.stopChangesFeed()

            logging.debug("[changes] I made it out of the changes loop!")

    def __isTriggerDocAssignedToMe(self, doc):
        if "worker" in doc:
            return doc["worker"] == self.workerId
        else:
            return self.workerId == "worker0"

    def stopChangesFeed(self):
        if self.changes != None:
            self.changes.stop()
            self.changes = None

    def createAndRunConsumer(self, doc):
        triggerFQN = doc['_id']

        if doc['isMessageHub']:
            retries = 0
            max_retries = 5
            url = '{}/admin/topics'.format(doc['kafka_admin_url'])
            username = doc['username']
            password = doc['password']
            topic = doc['topic']

            while retries < max_retries:
                headers = {'X-Auth-Token': '{}{}'.format(username, password)}
                response = self.session.get(url, headers=headers, stream=False, timeout=10.0, verify=check_ssl)
                status_code = response.status_code

                if status_code == 200:
                    topics = json.loads(response.content)

                    if topic in [t['name'] for t in topics]:
                        break
                    else:
                        logging.warn('Topic {} for trigger {} no longer exists. This consumer will not be created and trigger will be disabled'.format(topic, triggerFQN))
                        reason = 'Topic does not exist. You must create the topic first: {}.'.format(url, topic)
                        self.database.disableTrigger(triggerFQN, 404, reason)
                        return
                elif status_code == 403:
                    logging.warn('[{}] Invalid authKey.  This consumer will not be created and trigger will be disabled'.format(triggerFQN))
                    reason = 'Invalid authKey. {} returned 403 using authKey {}{}.'.format(url, username, password)
                    self.database.disableTrigger(triggerFQN, 403, reason)
                    return
                else:
                    sleepyTime = pow(2, retries)
                    logging.info('Received status code {} for {} while validating authKey. Retrying in {} second(s)'.format(status_code, triggerFQN, sleepyTime))
                    time.sleep(sleepyTime)
                    retries += 1

        # Create a representation for this trigger, even if it is disabled
        # This allows it to appear in /health as well as allow it to be deleted
        # Creating this object is lightweight and does not initialize any connections
        consumer = Consumer(triggerFQN, doc)
        self.consumers.addConsumerForTrigger(triggerFQN, consumer)

        logging.info('[{}] Starting consumer...'.format(triggerFQN))
        consumer.start()

    def __isTriggerDocActive(self, doc):
        return ('status' not in doc or doc['status']['active'] == True)


class CanaryDocumentGenerator (Thread):
    def __init__(self):
        Thread.__init__(self)
        self.daemon = True
        self.database = Database()

    def run(self):
        while True:
            # create a new canary document every so often
            self.database.createCanary()
            time.sleep(canaryInterval)

        logging.error('[canary generator] Exited the main loop!')
