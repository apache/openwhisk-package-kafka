# Copyright 2015 IBM Corp. All Rights Reserved.
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
import requests
import ssl
import threading
import time
from database import Database
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
from kafka.structs import OffsetAndMetadata


class Consumer (threading.Thread):
    retry_timeout = 1   # Timeout in seconds
    max_retries = 10    # Maximum number of times to retry firing trigger

    def __init__(self, trigger, params):
        threading.Thread.__init__(self)

        self.daemon = True
        self.shouldRun = True
        self.trigger = trigger
        self.isMessageHub = params["isMessageHub"]
        self.triggerURL = params["triggerURL"]
        self.brokers = params["brokers"]
        self.topic = params["topic"]
        self.username = params["username"]
        self.password = params["password"]

        # handle the case where there may be existing triggers that do not
        # have the isJSONData field set
        if "isJSONData" in params:
            self.parseAsJson = params["isJSONData"]
        else:
            self.parseAsJson = False

    def run(self):

        # TODO may need different code paths for Message Hub vs generic Kafka
        if self.isMessageHub:
            sasl_mechanism = 'PLAIN'       # <-- changed from 'SASL_PLAINTEXT'
            security_protocol = 'SASL_SSL'

            # Create a new context using system defaults, disable all but TLS1.2
            context = ssl.create_default_context()
            context.options &= ssl.OP_NO_TLSv1
            context.options &= ssl.OP_NO_TLSv1_1

            # this initialization can take some time, might as well have it in
            # the run method so it doesn't block the application
            self.consumer = KafkaConsumer(self.topic,
                                          group_id=self.trigger,
                                          bootstrap_servers=self.brokers,
                                          sasl_plain_username=self.username,
                                          sasl_plain_password=self.password,
                                          security_protocol=security_protocol,
                                          ssl_context=context,
                                          sasl_mechanism=sasl_mechanism,
                                          auto_offset_reset="latest",
                                          enable_auto_commit=False)
        else:
            self.consumer = KafkaConsumer(self.topic,
                                          group_id=self.trigger,
                                          client_id="openwhisk",
                                          bootstrap_servers=self.brokers,
                                          auto_offset_reset="latest",
                                          enable_auto_commit=False)

        logging.info("[{}] Now listening in order to fire trigger".format(self.trigger))

        while self.shouldRun:
            partition = self.consumer.poll(1000)
            if len(partition) > 0:
                logging.debug("partition: {}".format(partition))
                topic = partition[partition.keys()[0]]  # this assumes we only ever listen to one topic per consumer
                messages = []
                messageSize = 0

                for message in topic:
                    logging.debug("Consumed message: {}".format(str(message)))
                    messageSize += len(message.value)
                    fieldsToSend = {
                        'value': self.parseMessageIfNeeded(message.value),
                        'topic': message.topic,
                        'partition': message.partition,
                        'offset': message.offset,
                        'timestamp': message.timestamp,
                        'timestamp_type': message.timestamp_type,
                        'key': message.key
                    }
                    messages.append(fieldsToSend)

                payload = {}
                payload['messages'] = messages
                retry = True
                retry_count = 0

                logging.info("[{}] Firing trigger with {} messages with a total size of {} bytes".format(self.trigger,
                     len(messages), messageSize))

                while retry:
                    try:
                        response = requests.post(self.triggerURL, json=payload)
                        status_code = response.status_code
                        logging.info("[{}] Repsonse status code {}".format(self.trigger, status_code))

                        # Manually commit offset if the trigger was fired successfully. Retry firing the trigger if the
                        # service was unavailable (503), an internal server error occurred (500), the request timed out
                        # (408), or the request was throttled (429).
                        if status_code == 200:
                            self.consumer.commit()
                            retry = False
                        elif status_code not in [503, 500, 408, 429]:
                            logging.error('[{}] Error talking to OpenWhisk, status code {}'.format(self.trigger,
                                status_code))
                            self.shouldRun = False
                            retry = False
                    except requests.exceptions.RequestException as e:
                        logging.error('[{}] Error talking to OpenWhisk: {}'.format(self.trigger, e))

                    if retry:
                        retry_count += 1

                        if retry_count < self.max_retries:
                            logging.info("[{}] Retrying in {} second(s)".format(self.trigger, self.retry_timeout))
                            time.sleep(self.retry_timeout)
                        else:
                            logging.info("[{}] Skipping {} messages to offset {} of partition {}".format(self.trigger,
                                len(messages), message.offset, message.partition))
                            self.consumer.commit()
                            retry = False

        if not self.shouldRun:
            logging.info("[{}] Consumer exiting main loop".format(self.trigger))
            self.consumer.unsubscribe()
            self.consumer.close()
            database = Database()
            database.deleteTrigger(self.trigger)

    def shutdown(self):
        logging.info("[{}] Shutting down consumer for trigger".format(self.trigger))
        self.shouldRun = False
        self.join()

    def parseMessageIfNeeded(self, value):
        if self.parseAsJson:
            try:
                parsed = json.loads(value)
                logging.info('[{}] Successfully parsed a message as JSON.'.format(self.trigger))
                return parsed
            except ValueError:
                # no big deal, just return the original value
                logging.warn('[{}] I was asked to parse a message as JSON, but I failed.'.format(self.trigger))
                pass

        logging.info('[{}] Returning un-parsed message'.format(self.trigger))
        return value
