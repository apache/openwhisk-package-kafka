function produceMessage(args, producerConfig) {
    const Kafka = require("node-rdkafka");

    // make sure we can ack
    producerConfig.dr_cb = true;

    var producer = new Kafka.Producer(producerConfig);

    var promise = new Promise(function (resolve, reject) {
        var deliveryAcknowledged = false;

        producer.on('delivery-report', function (report) {
            console.log('delivery-report: ' + JSON.stringify(report));
            deliveryAcknowledged = true;
        });

        producer.on('ready', function (arg) {
            console.log('Producer is ready ' + JSON.stringify(arg));

            var topic = producer.Topic(args.topic, {
                // Make the Kafka broker acknowledge our message (optional)
                'request.required.acks': args.required_acks ? args.required_acks : 1
            });

            var value = new Buffer(args.value);

            // if partition is set to -1, librdkafka will use the default partitioner
            var partition = args.partition ? args.partition : -1;

            console.log('Calling produce ' + args.topic + ' ' + value);
            producer.produce(topic, partition, value, args.key);

            //need to keep polling for a while to ensure the delivery reports are received
            var pollLoop = setInterval(function () {
                console.log('Calling poll');
                producer.poll();

                if (deliveryAcknowledged) {
                    console.log('Delivery acknowledged');
                    clearInterval(pollLoop);

                    console.log('Calling disconnect');
                    producer.disconnect();
                    resolve();
                }
            }, 200);
        });

        producer.on('error', function (msg) {
            reject({
                error: msg
            });
        });
    });

    console.log('Calling connect');
    producer.connect();

    return promise;
}

module.exports = produceMessage;
