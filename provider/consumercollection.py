"""ConsumerCollection class.

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

from threading import Lock

# basically a thread safe wrapper around dict
# I'm told that dict is already thread safe, but
# this is just for my own piece of mind.


class ConsumerCollection:

    def __init__(self):
        self.consumers = dict()
        self.lock = Lock()

    def getCopyForRead(self):
        with self.lock:
            copy = self.consumers.copy()

        return copy

    def hasConsumerForTrigger(self, triggerFQN):
        with self.lock:
            hasConsumer = triggerFQN in self.consumers

        return hasConsumer

    def getConsumerForTrigger(self, triggerFQN):
        with self.lock:
            consumer = self.consumers.get(triggerFQN)

        return consumer

    def addConsumerForTrigger(self, triggerFQN, consumer):
        with self.lock:
            self.consumers[triggerFQN] = consumer

    def removeConsumerForTrigger(self, triggerFQN):
        with self.lock:
            del self.consumers[triggerFQN]
