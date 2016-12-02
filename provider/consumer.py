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

import json
import logging
import os
import requests
import time

# HEADS UP! I'm importing confluent_kafka.Consumer as KafkaConsumer to avoid a
# naming conflict with my own Consumer class
from confluent_kafka import Consumer as KafkaConsumer, KafkaError
from database import Database
from datetime import datetime
from threading import Thread, Lock

local_dev = os.getenv('LOCAL_DEV', 'False')
check_ssl = (local_dev == 'False')

class Consumer:
    class State:
        Initializing = 'Initializing'
        Running = 'Running'
        Stopping = 'Stopping'
        Restart = 'Restart'
        Dead = 'Dead'

    def __init__(self, trigger, params):
        self.trigger = trigger
        self.params = params
        self.thread = ConsumerThread(trigger, params)

        # the following fields can be accessed from multiple threads
        # access needs to be protected with this Lock
        self.__lock = Lock()
        self.__restartCount = 0

        # this is weird.
        # The app needs this tho...
        self.triggerURL = params["triggerURL"]

    def currentState(self):
        return self.thread.currentState()

    def desiredState(self):
        return self.thread.desiredState()

    def shutdown(self):
        self.thread.shutdown()

    def start(self):
        self.thread.start()

    # should only be called by the Doctor thread
    def restart(self):
        if self.thread.desiredState() is Consumer.State.Dead:
            logging.info('[{}] Request to restart a consumer that is already slated for deletion.'.format(self.trigger))
            return

        with self.__lock:
            self.__restartCount += 1

        logging.info('[{}] Quietly shutting down consumer for restart'.format(self.trigger))
        self.thread.setDesiredState(Consumer.State.Restart)
        self.thread.join()
        logging.info('Consumer has shut down')

        # user may have interleaved a request to delete the trigger, check again
        if self.thread.desiredState() is not Consumer.State.Dead:
            logging.info('[{}] Starting new consumer thread'.format(self.trigger))
            self.thread = ConsumerThread(self.trigger, self.params)
            self.thread.start()

    def restartCount(self):
        with self.__lock:
            restartCount = self.__restartCount

        return restartCount

    def lastPoll(self):
        return self.thread.lastPoll()

    def secondsSinceLastPoll(self):
        return self.thread.secondsSinceLastPoll()


class ConsumerThread (Thread):

    retry_timeout = 1   # Timeout in seconds
    max_retries = 10    # Maximum number of times to retry firing trigger

    database = Database()

    def __init__(self, trigger, params):
        Thread.__init__(self)

        self.lock = Lock()

        # the following params may be set/read from other threads
        # only access through helper methods which handle thread safety!
        self.__currentState = Consumer.State.Initializing
        self.__desiredState = Consumer.State.Running
        self.__lastPoll = datetime.max

        self.daemon = True
        self.trigger = trigger
        self.isMessageHub = params["isMessageHub"]
        self.triggerURL = params["triggerURL"]
        self.brokers = params["brokers"]
        self.topic = params["topic"]

        if self.isMessageHub:
            self.username = params["username"]
            self.password = params["password"]

        # handle the case where there may be existing triggers that do not
        # have the isJSONData field set
        if "isJSONData" in params:
            self.parseAsJson = params["isJSONData"]
        else:
            self.parseAsJson = False

        # always init consumer to None in case the consumer needs to shut down
        # before the KafkaConsumer is fully initialized/assigned
        self.consumer = None

    # this only records the current state, and does not affect a state transition
    def __recordState(self, newState):
        with self.lock:
            self.__currentState = newState

    def currentState(self):
        with self.lock:
            state = self.__currentState

        return state

    def setDesiredState(self, newState):
        logging.info('[{}] Request to set desiredState to {}'.format(self.trigger, newState))

        with self.lock:
            if self.__desiredState is Consumer.State.Dead and newState is not Consumer.State.Dead:
                logging.info('[{}] Asking to kill a consumer that is already marked for death. Doing nothing.'.format(self.trigger))
                return
            else:
                logging.info('[{}] Setting desiredState to: {}'.format(self.trigger, newState))
                self.__desiredState = newState

    def desiredState(self):
        with self.lock:
            state = self.__desiredState

        return state

    # convenience method for checking if desiredState is Running
    def __shouldRun(self):
        return self.desiredState() is Consumer.State.Running

    def lastPoll(self):
        with self.lock:
            lastPoll = self.__lastPoll

        return lastPoll

    def updateLastPoll(self):
        with self.lock:
            self.__lastPoll = datetime.now()

    def secondsSinceLastPoll(self):
        lastPollDelta = datetime.now() - self.lastPoll()
        return lastPollDelta.total_seconds()

    def run(self):
        try:
            self.consumer = self.__createConsumer()

            while self.__shouldRun():
                messages = self.__pollForMessages()

                if len(messages) > 0:
                    self.__fireTrigger(messages)

            logging.info("[{}] Consumer exiting main loop".format(self.trigger))

            if self.desiredState() == Consumer.State.Dead:
                logging.info('[{}] Permanently killing consumer because desired state is Dead'.format(self.trigger))
                self.database.deleteTrigger(self.trigger)
            elif self.desiredState() == Consumer.State.Restart:
                logging.info('[{}] Quietly letting the consumer thread stop in order to allow restart.'.format(self.trigger))
                # nothing else to do because this Thread is about to go away
            else:
                # uh-oh... this really shouldn't happen
                logging.error('[{}] Consumer stopped without being asked'.format(self.trigger))
        except Exception as e:
            logging.error('[{}] Uncaught exception: {}'.format(self.trigger, e))

        try:
            if self.consumer is not None:
                logging.info('[{}] Cleaning up consumer'.format(self.trigger))
                logging.debug('[{}] Closing KafkaConsumer'.format(self.trigger))
                self.consumer.unsubscribe()
                self.consumer.close()
                logging.debug('[{}] Successfully closed KafkaConsumer'.format(self.trigger))

                logging.debug('[{}] Dellocating KafkaConsumer'.format(self.trigger))
                self.consumer = None
        except Exception as e:
            logging.error('[{}] Uncaught exception while shutting down consumer: {}'.format(self.trigger, e))
        finally:
            logging.info('[{}] Recording consumer as Dead. Bye bye!'.format(self.trigger))
            self.__recordState(Consumer.State.Dead)

    def __createConsumer(self):
        if self.__shouldRun():
            config = {'metadata.broker.list': ','.join(self.brokers),
                        'group.id': self.trigger,
                        'default.topic.config': {'auto.offset.reset': 'latest'},
                        'enable.auto.commit': False
                    }

            if self.isMessageHub:
                # append Message Hub specific config
                config.update({'ssl.ca.location': '/etc/ssl/certs/',
                                'sasl.mechanisms': 'PLAIN',
                                'sasl.username': self.username,
                                'sasl.password': self.password,
                                'security.protocol': 'sasl_ssl'
                             })

            consumer = KafkaConsumer(config)
            consumer.subscribe([self.topic])
            logging.info("[{}] Now listening in order to fire trigger".format(self.trigger))
            return consumer

    def __pollForMessages(self):
        messages = []
        messageSize = 0
        batchMessages = True

        if self.__shouldRun():
            while batchMessages and (self.secondsSinceLastPoll() < 2):
                message = self.consumer.poll(1.0)

                if self.secondsSinceLastPoll() < 0:
                    self.__recordState(Consumer.State.Running)

                if (message is not None):
                    if not message.error():
                        logging.debug("Consumed message: {}".format(str(message)))
                        messageSize += len(message.value())
                        messages.append(message)
                    elif message.error().code() != KafkaError._PARTITION_EOF:
                        logging.error('[{}] Error polling: {}'.format(self.trigger, message.error()))
                        batchMessages = False
                    else:
                        logging.debug('[{}] No more messages. Stopping batch op.'.format(self.trigger))
                        batchMessages = False
                else:
                    logging.debug('[{}] message was None. Stopping batch op.'.format(self.trigger))
                    batchMessages = False

        logging.debug('[{}] Completed poll'.format(self.trigger))

        if len(messages) > 0:
            logging.info("[{}] Found {} messages with a total size of {} bytes".format(self.trigger, len(messages), messageSize))

        self.updateLastPoll()
        return messages

    def __fireTrigger(self, messages):
        if self.__shouldRun():
            lastMessage = messages[len(messages) - 1]

            # I'm sure there is a much more clever way to do this ;)
            mappedMessages = []
            for message in messages:
                fieldsToSend = {
                    'value': self.__parseMessageIfNeeded(message.value()),
                    'topic': message.topic(),
                    'partition': message.partition(),
                    'offset': message.offset(),
                    'key': message.key()
                }
                mappedMessages.append(fieldsToSend)

            payload = {}
            payload['messages'] = mappedMessages
            retry = True
            retry_count = 0

            logging.info("[{}] Firing trigger with {} messages".format(self.trigger,len(messages)))

            while retry:
                try:
                    response = requests.post(self.triggerURL, json=payload, timeout=10.0, verify=check_ssl)
                    status_code = response.status_code
                    logging.info("[{}] Repsonse status code {}".format(self.trigger, status_code))

                    # Manually commit offset if the trigger was fired successfully. Retry firing the trigger if the
                    # service was unavailable (503), an internal server error occurred (500), the request timed out
                    # (408), or the request was throttled (429).
                    if status_code == 200:
                        self.consumer.commit()
                        retry = False
                    elif status_code not in [503, 500, 408, 429]:
                        logging.error('[{}] Error talking to OpenWhisk, status code {}'.format(self.trigger, status_code))

                        # abandon all hope?
                        self.setDesiredState(Consumer.State.Dead)
                        retry = False
                except requests.exceptions.RequestException as e:
                    logging.error('[{}] Error talking to OpenWhisk: {}'.format(self.trigger, e))

                if retry:
                    retry_count += 1

                    if retry_count < self.max_retries:
                        logging.info("[{}] Retrying in {} second(s)".format(
                            self.trigger, self.retry_timeout))
                        time.sleep(self.retry_timeout)
                    else:
                        logging.warn("[{}] Skipping {} messages to offset {} of partition {}".format(self.trigger, len(messages), lastMessage.offset, lastMessage.partition))
                        self.consumer.commit()
                        retry = False

    def shutdown(self):
        logging.info("[{}] Shutting down consumer for trigger".format(self.trigger))
        self.setDesiredState(Consumer.State.Dead)
        self.database.deleteTrigger(self.trigger)

    def __parseMessageIfNeeded(self, value):
        if self.parseAsJson:
            try:
                parsed = json.loads(value)
                logging.debug(
                    '[{}] Successfully parsed a message as JSON.'.format(self.trigger))
                return parsed
            except ValueError:
                # no big deal, just return the original value
                logging.warn(
                    '[{}] I was asked to parse a message as JSON, but I failed.'.format(self.trigger))
                pass

        logging.debug('[{}] Returning un-parsed message'.format(self.trigger))
        return value
