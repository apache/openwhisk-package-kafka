<!--
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
-->

## MessageHub Trigger Provider Architecture

### Create Trigger Feed
![MessageHub Trigger Create](images/Arch-Provider-MHV1-Create.png)

**Scenario:** User wants to create a trigger `trigger1` for MessageHub service instance `instance1`, using Credentials `Credential-1` and use rule `rule1` to invoke action `action1` with messages from topic `topic1`.

1. Developer creates a MessageHub service `instance1`
2. Developer creates topic `topic1` in MessageHub service `instance1`
3. Developer creates Credential key `Credential-1` for MessageHub `instance1`
4. Developer creates trigger `trigger1` on OpenWhisk, the trigger stores the annotation `feed` with the feedAction name from system package or binded package.(`/whisk.system/messagingWeb/messageHubFeed`).
5. Developer invokes action feedAction to create trigger feed passing input parameters (lifeCycle:`CREATE`, `trigger1`, Credentials1, Options:`topic1`)
6. The feedAction invokes feedWebAction forwarding input parameter.
7. The feedWebAction inserts trigger feed doc into the DB for worker group 0 (feedWebAction protects DB credentials)
8. DB insertion notifies workers group 0 via Cloudant/CouchDB changes API, workers listen on DB view with a filter for their group `worker0` and gets the DB doc.
9. Kafka Consumer is created on each worker in a consumer group and starts polling for messages on `topic1` from `instance1` using `Credentials-1`.
10. Developer creates `rule1` indicating that when `trigger1` fires invoke `action1`.
11. Event source produces messages on `topic1`.
12. Both consumers will batch the messages from `topic1` and fire `trigger1`.
    - The fire is done with an http request containing the batch of messages in the body.
    - Consumer will not poll for more messages and will not commit batch of messages until the http request gets a response from OpenWhisk trigger endpoint.
    - Consumers in the same consumer group get assigned a set of partitions, each consumer on each worker host will get a unique set of messages avoiding duplicate messages being included in trigger fires.
9. OpenWhisk will process the trigger fire for `trigger1` and finds the `rule1` and invokes `action1` with messages from topic `topic1`.

### Update Trigger Feed
![MessageHub Trigger Update](images/Arch-Provider-MHV1-Update.png)

**Scenario:** User wants to update trigger `trigger1` to change from topic `topic1` to topic `topic2`.

1. Developer creates topic `topic2` in MessageHub service `instance1`.
2. Developer gets the annotation `feed` from trigger `trigger1`.
3. Developer invokes feedAction to update trigger feed passing input parameters (lifeCycle:`UPDATE`, `trigger1`, Options:`topic2`).
4. The feedAction invokes feedWebAction forwarding input parameter.
5. The feedWebAction inserts trigger feed doc into the DB for worker group 0 (feedWebAction protects DB credentials).
6. DB insertion notifies workers group 0 via Cloudant/CouchDB changes API, workers listen on DB view with a filter for their group `worker0` and gets the DB doc.
7. Kafka Consumer is re-created on each worker in a consumer group and starts polling for messages on `topic2` from `instance1` using `Credentials-1`.
8. Event source produces messages on `topic2`.
9. Both consumers will now handle `topic2` instead of `topic1`.
10. OpenWhisk will process the trigger fire for `trigger1`, finds the rule `rule1` and invokes `action1` with messages from topic `topic2`.

### Read Trigger Feed
![MessageHub Trigger Read](images/Arch-Provider-MHV1-Read.png)

**Scenario:** User wants to read the configuration and status for trigger `trigger1`.

1. Developer gets the annotation `feed` from trigger `trigger1`.
2. Developer invokes feedAction to read the trigger feed passing input parameters (lifeCycle:`READ`, `trigger1`).
3. The feedAction invokes feedWebAction forwarding input parameter.
4. The feedWebAction gets the trigger feed doc from the DB (feedWebAction protects DB credentials).
5. The DB returns the trigger feed doc for `trigger1`.
6. The feedWebAction returns a response to feedAction.
7. The feedAction returns response (config, status) to Developer.

### Delete Trigger Feed
![MessageHub Trigger Read](images/Arch-Provider-MHV1-Delete.png)

**Scenario:** User wants to delete trigger `trigger1`.

1. Developer deletes rule `rule1`
2. Developer gets the annotation `feed` from trigger `trigger1`.
3. Developer invokes feedAction to delete the trigger feed passing input parameters (lifeCycle:`DELETE`, `trigger1`).
4. The feedAction invokes feedWebAction forwarding input parameter.
5. The feedWebAction updates the trigger feed doc into the DB with a field `delete:true`(feedWebAction protects DB credentials).
6. DB update notifies workers group 0 via Cloudant/CouchDB changes API, workers listen on DB view with a filter for their group `worker0` and gets the DB doc. The Kafka consumers for `trigger1/topic2` get destroyed.
7. The feedWebAction deletes the trigger feed doc from the DB.
8. The Developer deletes trigger `trigger1`
