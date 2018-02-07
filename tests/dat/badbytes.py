
import ssl
from kafka import KafkaProducer

def main(params):

    topic = params['topic']
    brokers = params['brokers']
    username = params['username']
    password = params['password']

    context = ssl.create_default_context()
    context.options &= ssl.OP_NO_TLSv1
    context.options &= ssl.OP_NO_TLSv1_1

    producer = KafkaProducer(
        api_version=(0, 10),
        batch_size=0,
        bootstrap_servers=brokers,
        sasl_plain_username=username,
        sasl_plain_password=password,
        security_protocol='SASL_SSL',
        ssl_context=context,
        sasl_mechanism='PLAIN'
    )

    future = producer.send(topic, key='foo', value='\xb6bar')
    sent = future.get(timeout=60)
    msg = "Successfully sent message to {}:{} at offset {}".format(sent.topic, sent.partition, sent.offset)
    
    return {"success": True, "message": msg}
