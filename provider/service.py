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
import time

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


class Service (Thread):
    def __init__(self, consumers):
        Thread.__init__(self)
        self.daemon = True

        self.database = None
        self.lastSequence = None
        self.canaryGenerator = CanaryDocumentGenerator()

        self.consumers = consumers

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

                            if not self.consumers.hasConsumerForTrigger(change["id"]):
                                logging.info('[{}] Found a new trigger to create'.format(change["id"]))
                                self.createAndRunConsumer(document)
                            else:
                                logging.info('[{}] Found a change to an existing trigger'.format(change["id"]))
                                existingConsumer = self.consumers.getConsumerForTrigger(change["id"])

                                if existingConsumer.desiredState() == Consumer.State.Disabled and self.__isTriggerDocActive(document):
                                    # disabled trigger has become active
                                    logging.info('[{}] Existing disabled trigger should become active'.format(change["id"]))
                                    self.createAndRunConsumer(document)
                                elif existingConsumer.desiredState() == Consumer.State.Running and not self.__isTriggerDocActive(document):
                                    # running trigger should become disabled
                                    logging.info('[{}] Existing running trigger should become disabled'.format(change["id"]))
                                    existingConsumer.disable()
                                else:
                                    logging.debug('[changes] Found non-interesting trigger change: \n{}\n{}'.format(existingConsumer.desiredState(), document))
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

    def stopChangesFeed(self):
        if self.changes != None:
            self.changes.stop()
            self.changes = None

    def createAndRunConsumer(self, doc):
        triggerFQN = doc['_id']

        # Create a representation for this trigger, even if it is disabled
        # This allows it to appear in /health as well as allow it to be deleted
        # Creating this object is lightweight and does not initialize any connections
        consumer = Consumer(triggerFQN, doc)
        self.consumers.addConsumerForTrigger(triggerFQN, consumer)

        if self.__isTriggerDocActive(doc):
            logging.info('[{}] Trigger was determined to be active, starting...'.format(triggerFQN))
            consumer.start()
        else:
            logging.info('[{}] Trigger was determined to be disabled, not starting...'.format(triggerFQN))

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
