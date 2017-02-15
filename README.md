# OpenWhisk package for communication with Kafka or IBM Message Hub

This project is an OpenWhisk package that allows you to communicate with Kafka or IBM Message Hub instances for publishing and consuming messages using native high performance Kafka API.

OpenWhisk is a cloud-first distributed event-based programming service and represents an event-action platform that allows you to execute code in response to an event. These events can come from many different sources, such as Object Storage, direct HTTP, Cloudant database, etc. One of the popular event sources is Message Hub and Kafka, both of which can also be used as an effective instriment to publish events from OpenWhisk to the rest of the world and allow for highly scalable and performant asyncronous communication for event driven applications.

## Usage
This package allows you to create triggers that react when messages are posted to either an IBM Message Hub instance, or to a generic Kafka instance. Since the parameters required for each of these situations are different, there are two separate feeds to handle them: `/messaging/messageHubFeed` and `messaging/kafkaFeed`.

Additionally, two actions are included which allow you to produce messages to either Message Hub, or generic Kafka instances. These are `/messaging/messageHubProduce`, and `/messaging/kafkaProduce` respectively.

### Creating a Trigger that listens to an IBM MessageHub instance
In order to create a trigger that reacts when messages are posted to a Message Hub instance, you need to use the feed named `messageHubFeed`. This feed supports the following parameters:
<!-- I removed the "messaging" from the feed name above because as you show below in the sample and I verified in the code - you do not put "messaging" in front of the feed name - instead you put credentials - see sample below -->

|Name|Type|Description|
|---|---|---|
|kafka_brokers_sasl|JSON Array of Strings|This parameter is an array of `<host>:<port>` strings which comprise the brokers in your Message Hub instance|
|user|String|Your Message Hub user name|
|password|String|Your Message Hub password|
|topic|String|The topic you would like the trigger to listen to|
|kafka_admin_url|URL String|The URL of the Message Hub admin REST interface|
|isJSONData|Boolean (Optional - default=false)|When set to `true` this will cause the feed to attempt the message content as JSON before passing it along as the trigger payload.|

While this list of parameters may seem daunting, they can be automatically set for you by using the package refresh CLI command:

```wsk package refresh```

However, if you want to create the trigger manually, it would look something like:
```
wsk trigger create MyMessageHubTrigger -f /whisk.system/messaging/messageHubFeed -p kafka_brokers_sasl "[\"kafka01-prod01.messagehub.services.us-south.bluemix.net:9093\", \"kafka02-prod01.messagehub.services.us-south.bluemix.net:9093\", \"kafka03-prod01.messagehub.services.us-south.bluemix.net:9093\"]" -p topic mytopic -p user <your Message Hub user> -p password <your Message Hub password> -p kafka_admin_url https://kafka-admin-prod01.messagehub.services.us-south.bluemix.net:443 -p isJSONData true
```

### Creating a Trigger that listens to a Generic Kafka instance
In order to create a trigger that reacts when messages are posted to an unauthenticated Kafka instance, you need to use the feed named `messaging/kafkaFeed`. This feed supports the following parameters:

|Name|Type|Description|
|---|---|---|
|brokers|JSON Array of Strings|This parameter is an array of `<host>:<port>` strings which comprise the brokers in your Message Hub instance|
|topic|String|The topic you would like the trigger to listen to|
|isJSONData|Boolean (Optional - default=false)|When set to `true` this will cause the feed to attempt the message content as JSON before passing it along as the trigger payload.|

Example:
```
wsk trigger create MyKafkaTrigger -f /whisk.system/messaging/kafkaFeed -p brokers "[\"mykafkahost:9092\", \"mykafkahost:9093\"]" -p topic mytopic -p isJSONData true
```

### Ok, what happens now?
After creating a trigger, the system will monitor the specified topic in your messaging service. When new messages are posted, the trigger will be fired. 

The payload of that trigger will contain a `messages` field which is an array of messages that have been posted since the last time your trigger fired. Each message object in the array will contain the following fields:
- topic
- partition
- offset
- key
- value

In Kafka terms, these fields should be self-evident. However, the `value` requires special consideration. If the `isJSONData` parameter was set `false` (or not set at all) when the trigger was created, the `value` field will be the raw value of the posted message. However, if `isJSONData` was set to `true` when the trigger was created, the system will make attempt to parse this value as a JSON object, on a best-effort basis. If parsing is successful, then the `value` in the trigger payload will be the resulting JSON object.

For example, if a message of `{"title": "Some string", "amount": 5, "isAwesome": true}` is posted with `isJSONData` set to `true`, the trigger payload might look something like this:

```JSON
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

```JSON
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

### Messages are batched
You will notice that the trigger payload contains an array of messages. This means that if you are producing messages to your messaging system very quickly, the feed will attempt to batch up the posted messages into a single firing of your trigger. This allows the messages to be posted to your trigger more rapidly and efficiently.

Please keep in mind when coding actions that are fired by your trigger, that the number of messages in the payload is technically unbounded, but will always be greater than 0. Here is an example of a batched message (please note the change in the *offset* value):
 
 ```
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

### Producing messages to Message Hub
If you would like to use an OpenWhisk action to conveniently produce a message to Message Hub, you can use the `/messaging/messageHubProduce` action. This action takes the following parameters:

|Name|Type|Description|
|---|---|---|
|kafka_brokers_sasl|JSON Array of Strings|This parameter is an array of `<host>:<port>` strings which comprise the brokers in your Message Hub instance|
|user|String|Your Message Hub user name|
|password|String|Your Message Hub password|
|topic|String|The topic you would like the trigger to listen to|
|value|String|The value for the message you would like to produce|
|key|String (Optional)|The key for the message you would like to produce|

While the first three parameters can be automatically bound by using `wsk package refresh`, here is an example of invoking the action with all required parameters:

```
wsk action invoke /messaging/messageHubProduce -p kafka_brokers_sasl "[\"kafka01-prod01.messagehub.services.us-south.bluemix.net:9093\", \"kafka02-prod01.messagehub.services.us-south.bluemix.net:9093\", \"kafka03-prod01.messagehub.services.us-south.bluemix.net:9093\"]" -p topic mytopic -p user <your Message Hub user> -p password <your Message Hub password> -p value "This is the content of my message"
```

### Producing messages to a generic Kafka instance
If you would like to use an OpenWhisk action to conveniently produce a message to a generic Kafka instance, you can use the `/messaging/kafkaProduce` action. This action takes the following parameters:

|Name|Type|Description|
|---|---|---|
|brokers|JSON Array of Strings|This parameter is an array of `<host>:<port>` strings which comprise the brokers in your Kafka cluster|
|topic|String|The topic you would like the trigger to listen to|
|value|String|The value for the message you would like to produce|
|key|String (Optional)|The key for the message you would like to produce|

Here is an example of invoking the action with all required parameters:

```
wsk action invoke /messaging/kafkaProduce -p brokers "[\"mykafkahost:9092\", \"mykafkahost:9093\"]" -p topic mytopic -p value "This is the content of my message"
```

## Examples

### Producing messages to a Message Hub or generic Kafka instances using DIY action
Before the time when OpenWhisk had built-in action to publish messages to and from Message Hub and Kafka developers used to accomplish this functionality using custom built actions using any of the OpenWhisk supported programming languages. This is still possible today and if, for some reason you are not satisfied with OpenWhisk provided Message Hub and Kafka feed and publish action, you can still develop and use your own. While creating such action, you can use Message Hub and Kafka Native API or Confluence REST API for Kafka. It is transparent to OpenWhisk which you use, just like with any other 3rd party API or library you decide to utilize in your OpenWhisk action. This [sample project](https://github.com/IBM/ogs-data-processing-messagehub) demonstrates the use of OpenWhisk for sending and receiving IBM Message Hub and Kafka messages. 

### Integrating OpenWhisk with IBM Message Hub, Node Red, IBM Watson IoT, IBM Object Storage, and IBM Data Science Experience
Example that integrates OpenWhisk with IBM Message Hub, Node Red, IBM Watson IoT, IBM Object Storage, IBM Data Science Experience (Spark) service can be [found here](https://medium.com/openwhisk/transit-flexible-pipeline-for-iot-data-with-bluemix-and-openwhisk-4824cf20f1e0).
 
## Testing
An OpenWhisk deployment is required in order for the automated test suite to be executed. To run tests locally, run ```$ ./gradlew tests:test -Dhost=<container_address> -Dport=<docker_port>``` from the project's root directory. Where ```<docker_address>``` is the IP address of the service's docker container, and ```<docker_port>``` is the port the docker container is listening on. In addition, ```OPENWHISK_HOME``` must be set to the root of the local OpenWhisk directory. Ex: ```export OPENWHISK_HOME=<openwhisk_directory>```.

## References
- [OpenWhisk](https://www.ibm.com/cloud-computing/bluemix/openwhisk)
- [IBM Message Hub](https://developer.ibm.com/messaging/message-hub/)
- [Apache Kafka](https://kafka.apache.org/)
