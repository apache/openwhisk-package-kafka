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

# Apache OpenWhisk package for communication with Kafka or IBM Message Hub

[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](http://www.apache.org/licenses/LICENSE-2.0)
[![Build Status](https://travis-ci.com/apache/openwhisk-package-kafka.svg?branch=master)](https://travis-ci.com/github/apache/openwhisk-package-kafka)

This project is an OpenWhisk package that allows you to communicate with Kafka or IBM Message Hub instances for publishing and consuming messages using native high performance Kafka API.

OpenWhisk is a cloud-first distributed event-based programming service and represents an event-action platform that allows you to execute code in response to an event. These events can come from many different sources, such as Object Storage, direct HTTP, Cloudant database, etc. One of the popular event sources is Message Hub and Kafka, both of which can also be used as an effective instrument to publish events from OpenWhisk to the rest of the world and allow for highly scalable and performant asynchronous communication for event driven applications.

## Using the Messaging package
This package allows you to create triggers that react when messages are posted to either an [IBM Message Hub](https://developer.ibm.com/messaging/message-hub/) instance, or to a generic Kafka instance. Since the parameters required for each of these situations are different, there are two separate feeds to handle them: `/messaging/messageHubFeed` and `messaging/kafkaFeed`.

Additionally, two actions are included which allow you to produce messages to either Message Hub, or generic Kafka instances. These are `/messaging/messageHubProduce`, and `/messaging/kafkaProduce` respectively.

### Creating a Trigger that listens to an IBM MessageHub instance
In order to create a trigger that reacts when messages are posted to a Message Hub instance, you need to use the feed named `/messaging/messageHubFeed`. This feed action supports the following parameters:

|Name|Type|Description|
|---|---|---|
|kafka_brokers_sasl|JSON Array of Strings|This parameter is an array of `<host>:<port>` strings which comprise the brokers in your Message Hub instance|
|user|String|Your Message Hub user name|
|password|String|Your Message Hub password|
|topic|String|The topic you would like the trigger to listen to|
|kafka_admin_url|URL String|The URL of the Message Hub admin REST interface|
|isJSONData|Boolean (Optional - default=false)|When set to `true` this will cause the provider to attempt to parse the message value as JSON before passing it along as the trigger payload.|
|isBinaryKey|Boolean (Optional - default=false)|When set to `true` this will cause the provider to encode the key value as Base64 before passing it along as the trigger payload.|
|isBinaryValue|Boolean (Optional - default=false)|When set to `true` this will cause the provider to encode the message value as Base64 before passing it along as the trigger payload.|

While this list of parameters may seem daunting, they can be automatically set for you by using the package refresh CLI command:

1. Create an instance of Message Hub service under your current organization and space that you are using for OpenWhisk.

2. Verify that the topic you want to listen to already exists in Message Hub or create a new topic, for example `mytopic`.

3. Refresh the packages in your namespace. The refresh automatically creates a package binding for the Message Hub service instance that you created.

  ```
  $ wsk package refresh
  ```
  ```
  created bindings:
  Bluemix_Message_Hub_Credentials-1
  ```

  ```
  $ wsk package list
  ```
  ```
  packages
  /myBluemixOrg_myBluemixSpace/Bluemix_Message_Hub_Credentials-1 private
  ```

  Your package binding now contains the credentials associated with your Message Hub instance.

4. Now all you need to do is create a Trigger that will be fired when new messages are posted to your Message Hub topic.

  ```
  $ wsk trigger create MyMessageHubTrigger -f /myBluemixOrg_myBluemixSpace/Bluemix_Message_Hub_Credentials-1/messageHubFeed -p topic mytopic
  ```

### Setting up a Message Hub package outside Bluemix

If you're not using OpenWhisk in Bluemix or if you want to set up your Message Hub outside of Bluemix, you must manually create a package binding for your Message Hub service. You need the Message Hub service credentials and connection information.

1. Create a package binding that is configured for your Message Hub service.

  ```
  $ wsk package bind /whisk.system/messaging myMessageHub -p kafka_brokers_sasl "[\"kafka01-prod01.messagehub.services.us-south.bluemix.net:9093\", \"kafka02-prod01.messagehub.services.us-south.bluemix.net:9093\", \"kafka03-prod01.messagehub.services.us-south.bluemix.net:9093\"]" -p user <your Message Hub user> -p password <your Message Hub password> -p kafka_admin_url https://kafka-admin-prod01.messagehub.services.us-south.bluemix.net:443
  ```

2. Now you can create a Trigger using your new package that will be fired when new messages are posted to your Message Hub topic.

  ```
  $ wsk trigger create MyMessageHubTrigger -f myMessageHub/messageHubFeed -p topic mytopic -p isJSONData true
  ```

### Creating a Trigger that listens to a Generic Kafka instance
In order to create a trigger that reacts when messages are posted to an unauthenticated Kafka instance, you need to use the feed named `messaging/kafkaFeed`. This feed supports the following parameters:

|Name|Type|Description|
|---|---|---|
|brokers|JSON Array of Strings|This parameter is an array of `<host>:<port>` strings which comprise the brokers in your Message Hub instance|
|topic|String|The topic you would like the trigger to listen to|
|isJSONData|Boolean (Optional - default=false)|When set to `true` this will cause the provider to attempt to parse the message value as JSON before passing it along as the trigger payload.|
|isBinaryKey|Boolean (Optional - default=false)|When set to `true` this will cause the provider to encode the key value as Base64 before passing it along as the trigger payload.|
|isBinaryValue|Boolean (Optional - default=false)|When set to `true` this will cause the provider to encode the message value as Base64 before passing it along as the trigger payload.|

Example:
```
$ wsk trigger create MyKafkaTrigger -f /whisk.system/messaging/kafkaFeed -p brokers "[\"mykafkahost:9092\", \"mykafkahost:9093\"]" -p topic mytopic -p isJSONData true
```

### Using a separated kafka feed provider for each user
Sometimes users may not want to expose their kafka to the shared feed providers which are provided by OpenWhisk cloud provider.
They can run their own providers and use their own CouchDB/Cloudant by passing below additional parameters when **create/update/get/delete** triggers:

|Name|Type|Description|
|---|---|---|
|dedicated|Boolean|`true` to use dedicated kafka feed providers and CouchDB/Cloudant, default is `false`|
|DB_URL|URL|The base URL(including username:password) for persistent storage (either CouchDB or Cloudant)|
|DB_NAME|String|The database name for triggers|
|workers|An array of the IDs of the running instances with each ID of the form `workerX`. e.g. `["worker0", "worker1"]`|

Example:
```
$ wsk trigger create MyKafkaTrigger -f /whisk.system/messaging/kafkaFeed -p brokers "[\"mykafkahost:9092\", \"mykafkahost:9093\"]" -p topic mytopic -p isJSONData true -p dedicated true -p DB_URL http://admin:admin@localhost:5984 -p DB_NAME dedicated_triggers -p workers "[\"worker0\"]"
```

### Listening for messages
After creating a trigger, the system will monitor the specified topic in your messaging service. When new messages are posted, the trigger will be fired.

The payload of that trigger will contain a `messages` field which is an array of messages that have been posted since the last time your trigger fired. Each message object in the array will contain the following fields:
- topic
- partition
- offset
- key
- value

In Kafka terms, these fields should be self-evident. However, `key` has an optional feature `isBinaryKey` that allows the `key` to transmit binary data. Additionally, the `value` requires special consideration. Optional fields `isJSONData` and `isBinaryValue` are available to handle JSON and binary messages. These fields, `isJSONData` and `isBinaryValue`, cannot be used in conjunction with each other.

As an example, if `isBinaryKey` was set to `true` when the trigger was created, the `key` will be encoded as a Base64 string when returned from the payload of a fired trigger.

For example, if a `key` of `Some key` is posted with `isBinaryKey` set to `true`, the trigger payload will resemble the below:

```json
{
    "messages": [
        {
            "partition": 0,
            "key": "U29tZSBrZXk=",
            "offset": 421760,
            "topic": "mytopic",
            "value": "Some value"
        }
    ]
}
```

If the `isJSONData` parameter was set to `false` (or not set at all) when the trigger was created, the `value` field will be the raw value of the posted message. However, if `isJSONData` was set to `true` when the trigger was created, the system will attempt to parse this value as a JSON object, on a best-effort basis. If parsing is successful, then the `value` in the trigger payload will be the resulting JSON object.

For example, if a message of `{"title": "Some string", "amount": 5, "isAwesome": true}` is posted with `isJSONData` set to `true`, the trigger payload might look something like this:

```json
{
  "messages": [
    {
      "partition": 0,
      "key": null,
      "offset": 421760,
      "topic": "mytopic",
      "value": {
          "amount": 5,
          "isAwesome": true,
          "title": "Some string"
      }
    }
  ]
}
```
However, if the same message content is posted with `isJSONData` set to `false`, the trigger payload would look like this:

```json
{
  "messages": [
    {
      "partition": 0,
      "key": null,
      "offset": 421761,
      "topic": "mytopic",
      "value": "{\"title\": \"Some string\", \"amount\": 5, \"isAwesome\": true}"
    }
  ]
}
```

Similar to `isJSONData`, if `isBinaryValue` was set to `true` during trigger creation, the resultant `value` in the trigger payload will be encoded as a Base64 string.

For example, if a `value` of `Some data` is posted with `isBinaryValue` set to `true`, the trigger payload might look something like this:

```json
{
  "messages": [
    {
      "partition": 0,
      "key": null,
      "offset": 421760,
      "topic": "mytopic",
      "value": "U29tZSBkYXRh"
    }
  ]
}
```

If the same message is posted without `isBinaryData` set to `true`, the trigger payload would resemble the below example:

```json
{
  "messages": [
    {
      "partition": 0,
      "key": null,
      "offset": 421760,
      "topic": "mytopic",
      "value": "Some data"
    }
  ]
}
```

### Messages are batched
You will notice that the trigger payload contains an array of messages. This means that if you are producing messages to your messaging system very quickly, the feed will attempt to batch up the posted messages into a single firing of your trigger. This allows the messages to be posted to your trigger more rapidly and efficiently.

Please keep in mind when coding actions that are fired by your trigger, the number of messages in the payload will always be greater than 0. While there is technically no upper limit on the number of messages fired, limits are in place to ensure that each trigger payload is below the payload size limit defined by your OpenWhisk deployment.

Here is an example of a batched trigger payload (please note the change in the *offset* value):

 ```json
 {
   "messages": [
       {
         "partition": 0,
         "key": null,
         "offset": 100,
         "topic": "mytopic",
         "value": {
             "amount": 5
         }
       },
       {
         "partition": 0,
         "key": null,
         "offset": 101,
         "topic": "mytopic",
         "value": {
             "amount": 1
         }
       },
       {
         "partition": 0,
         "key": null,
         "offset": 102,
         "topic": "mytopic",
         "value": {
             "amount": 999
         }
       }
   ]
 }
 ```

### Checking the status and configuration of a trigger
The status and configuration of a feed trigger can be gotten using `wsk trigger get`.

Example:
```
$ wsk trigger get myTopicTrigger
```

This response will contain a `result` object containing the status of the trigger along with configuration information

e.g.

```json
{
  "result": {
      "config": {
          "isBinaryKey": false,
          "isBinaryValue": false,
          "isJSONData": false,
          "kafka_admin_url": ...,
          "kafka_brokers_sasl": [
              ...
          ],
          "user": ...,
          "password": ...,
          "topic": "myTopic",
          "triggerName": "/myNamespace/myTopicTrigger"
      },
      "status": {
          "active": true,
          "dateChanged": 1517245917340,
          "dateChangedISO": "2018-01-29T17:11:57Z"
      }
  }
}
```

 Triggers may become inactive when certain exceptional behavior occurs. For example, there was an error firing the trigger, or it was not possible to connect to the kafka brokers. When a trigger becomes inactive the status object will contain additional information as to the cause.

 e.g

 ```json
{
    "status": {
        "active": false,
        "dateChanged": 1517936358359,
        "dateChangedISO": "2018-02-06T16:59:18Z",
        "reason": {
            "kind": "AUTO",
            "message": "Automatically disabled trigger. Consumer was unable to connect to broker(s) after 30 attempts",
            "statusCode": 403
        }
    }
}
```

### Updating the configuration of a trigger
It is possible to update a limited set of configuration parameters for a trigger. The updatable parameters are:
- `isBinaryKey`
- `isBinaryValue`
- `isJSONData`

These parameters can be updated using `wsk trigger update`

Examples:

```
$ wsk trigger update myTopicTrigger -p isJSONData true
```

```
$ wsk trigger update myTopicTrigger -p isJSONData false -p isBinaryKey true -p isBinaryValue
```

### Producing messages to Message Hub

The `/messaging/messageHubProduce` Action is deprecated and will be removed at a future date. To maintain optimal performance, migrate your usage of the `/messaging/messageHubProduce` Action to use a persistent connection, for example, by deploying a non-OpenWhisk component which contains a Message Hub client.

The deprecated `/messaging/messageHubProduce` takes the following parameters:

|Name|Type|Description|
|---|---|---|
|kafka_brokers_sasl|JSON Array of Strings|This parameter is an array of `<host>:<port>` strings which comprise the brokers in your Message Hub instance|
|user|String|Your Message Hub user name|
|password|String|Your Message Hub password|
|topic|String|The topic you would like the trigger to listen to|
|value|String|The value for the message you would like to produce|
|key|String (Optional)|The key for the message you would like to produce|
|base64DecodeValue|Boolean (Optional - default=false)|If true, the message will be produced with a Base64 decoded version of the value parameter|
|base64DecodeKey|Boolean (Optional - default=false)|If true, the message will be produced with a Base64 decoded version of the key parameter|

While the first three parameters can be automatically bound by using `wsk package refresh`, here is an example of invoking the action with all required parameters:

```
wsk action invoke /messaging/messageHubProduce -p kafka_brokers_sasl "[\"kafka01-prod01.messagehub.services.us-south.bluemix.net:9093\", \"kafka02-prod01.messagehub.services.us-south.bluemix.net:9093\", \"kafka03-prod01.messagehub.services.us-south.bluemix.net:9093\"]" -p topic mytopic -p user <your Message Hub user> -p password <your Message Hub password> -p value "This is the content of my message"
```

### Producing messages to a generic Kafka instance

> :point_right: **Note** The `/messaging/kafkaProduce` Action is deprecated and will be removed at a future date. To maintain optimal performance, migrate your usage of the `/messaging/kafkaProduce` Action to use a persistent connection, for example, by deploying a non-OpenWhisk component which contains a Kafka Producer.

The deprecated `/messaging/kafkaProduce` takes the following parameters:

|Name|Type|Description|
|---|---|---|
|brokers|JSON Array of Strings|This parameter is an array of `<host>:<port>` strings which comprise the brokers in your Kafka cluster|
|topic|String|The topic you would like the trigger to listen to|
|value|String|The value for the message you would like to produce|
|key|String (Optional)|The key for the message you would like to produce|
|base64DecodeValue|Boolean (Optional - default=false)|If true, the message will be produced with a Base64 decoded version of the value parameter|
|base64DecodeKey|Boolean (Optional - default=false)|If true, the message will be produced with a Base64 decoded version of the key parameter|

Here is an example of invoking the action with all required parameters:

```
wsk action invoke /messaging/kafkaProduce -p brokers "[\"mykafkahost:9092\", \"mykafkahost:9093\"]" -p topic mytopic -p value "This is the content of my message"
```

## Producing Messages with Binary Content
You may find that you want to use one of the above actions to produce a message that has a key and/or value that is binary data. The problem is that invoking an OpenWhisk action inherently involves a REST call to the OpenWhisk server, which may require any binary parameter values of the action invocation to be Base64 encoded. How to handle this?

The action caller (you, or your code) must first Base64 encode the data, for example, the value of the message you want to produce. Pass this encoded data as the `value` parameter to the produce action. However, to ensure that the produced message's value contains the original bytes, you must also set the `base64DecodeValue` parameter to `true`. This will cause the produce action to first Base64 decode the `value` parameter before producing the message. The same procedure applies to producing messages with a binary key, using the `base64DecodeKey` parameter set to `true` in conjunction with a Base64 encoded `key` parameter.

## Examples

### Integrating OpenWhisk with IBM Message Hub, Node Red, IBM Watson IoT, IBM Object Storage, and IBM Data Science Experience
Example that integrates OpenWhisk with IBM Message Hub, Node Red, IBM Watson IoT, IBM Object Storage, IBM Data Science Experience (Spark) service can be [found here](https://medium.com/openwhisk/transit-flexible-pipeline-for-iot-data-with-bluemix-and-openwhisk-4824cf20f1e0).

## Architecture
Architecture documentation and diagrams, please refer to the [Architecture Docs](docs/arch/README.md)

## Development and Testing
If you wish to deploy the feed service yourself, please refer to the [Development Guide](docs/dev/README.md).

## References
- [OpenWhisk](https://www.ibm.com/cloud-computing/bluemix/openwhisk)
- [IBM Message Hub](https://developer.ibm.com/messaging/message-hub/)
- [Apache Kafka](https://kafka.apache.org/)

# Building from Source

To build this package from source, execute the command `./gradlew distDocker`
