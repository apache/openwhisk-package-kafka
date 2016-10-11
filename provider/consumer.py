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
from database import Database
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
from kafka.structs import OffsetAndMetadata


class Consumer (threading.Thread):

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

        logging.info("Now listening in order to fire trigger: {}".format(self.trigger))

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
                        'value': message.value,
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
                logging.info("Firing trigger {} with {} messages with a total size of {} bytes".format(self.trigger, len(messages), messageSize))

                try:
                    response = requests.post(self.triggerURL, json=payload)
                    status_code = response.status_code
                    logging.info("Repsonse status code {}".format(status_code))

                    if status_code >= 400 and status_code < 500:
                        # TODO need to kill this trigger
                        # However, 429 is the status code for a throttled response
                        logging.info('Need to kill this trigger due to status code {}'.format(status_code))
                    elif status_code >= 500:
                        # TODO OW server error... now what?
                        logging.error('Error talking to OW, status code {}'.format(status_code))
                    else:
                        # manually commit offsets only upon successfully
                        # firing the trigger
                        self.consumer.commit()
                except requests.exceptions.RequestException as e:
                    # TODO network issue talking to OW... now what?
                    logging.error('Error talking to OW: {}'.format(e))

                # TODO
                # error handling
                # retries? I suppose certain status codes warrant it...
                # only commit offset if post was successful?

        logging.info("Consumer for trigger {} exiting main loop".format(self.trigger))
        self.consumer.unsubscribe()
        self.consumer.close()

    def shutdown(self):
        logging.info("Shutting down consumer for trigger {}".format(self.trigger))
        self.shouldRun = False
        self.join()

        database = Database()
        database.deleteTrigger(self.trigger)
