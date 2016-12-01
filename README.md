This project is an OpenWhisk package that allows you to communicate with Kafka or IBM Message Hub instances.

## Status
This project is currently "beta". Most of the major work is done, and now we are putting on the final touches to get this ready for production use.

## Usage
This package allows you to create triggers that react when messages are posted to either an IBM Message Hub instance, or to a generic kafka instance. Since the parameters required for each of these situations are different, there are two separate feeds to handle them: `/messaging/messageHubFeed` and `messaging/kafkaFeed`.


### Creating a Trigger that Listens to an IBM MessageHub Instance
In order to create a trigger that reacts when messages are posted to a Message Hub instance, you need to use the feed named `messaging/messageHubFeed`. This feed supports the following parameters:

|Name|Type|Description|
|---|---|---|
|kafka_brokers_sasl|JSON Array of Strings|This parameter is an array of `<host>:<port>` strings which comprise the brokers in your Message Hub instance|
|user|String|Your Message Hub user name|
|password|String|Your Message Hub password|
|topic|String|The topic you would like the trigger to listen to|
|kafka_admin_url|URL String|The URL of the Message Hub admin REST interface|
|api_key|String|Your Message Hub API key|
|isJSONData|Boolean (Optional - default=false)|When set to `true` this will cause the feed to attempt the message content as JSON before passing it along as the trigger payload.|

While this list of parameters may seem daunting, they can be automatically set for you by using the package refresh CLI command:

```wsk package refresh```

However, if you want to create the trigger manually, it would look something like:
```
wsk trigger create MyMessageHubTrigger -f /whisk.system/messaging/messageHubFeed -p kafka_brokers_sasl "[\"kafka01-prod01.messagehub.services.us-south.bluemix.net:9093\", \"kafka02-prod01.messagehub.services.us-south.bluemix.net:9093\", \"kafka03-prod01.messagehub.services.us-south.bluemix.net:9093\"]" -p topic mytopic -p user <your Message Hub user> -p password <your Message Hub password> -p kafka_admin_url https://kafka-admin-prod01.messagehub.services.us-south.bluemix.net:443 -p api_key <your API key> -p isJSONData true
```

### Creating a Trigger that Listens to a Generic Kafka Instance
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

## Ok, what happens now?
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

## Messages are Batched
You will notice that the trigger payload contains an array of messages. This means that if you are producing messages to your messaging system very quickly, the feed will attempt to batch up the posted messages into a single firing of your trigger. This allows the messages to be posted to your trigger more rapidly and efficiently.

Please keep in mind when coding actions that are fired by your trigger, that the number of messages in the payload is technically unbounded, but will always be greater than 0.

## References
- [OpenWhisk](https://www.ibm.com/cloud-computing/bluemix/openwhisk)
- [IBM Message Hub](https://developer.ibm.com/messaging/message-hub/)
- [Apache Kafka](https://kafka.apache.org/)
