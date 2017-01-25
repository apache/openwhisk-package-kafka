import ssl
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

def main(params):
    validationResult = validateParams(params)
    if validationResult[0] != True:
        return {'error': validationResult[1]}

    sasl_mechanism = 'PLAIN'
    security_protocol = 'SASL_SSL'

    # Create a new context using system defaults, disable all but TLS1.2
    context = ssl.create_default_context()
    context.options &= ssl.OP_NO_TLSv1
    context.options &= ssl.OP_NO_TLSv1_1

    try:
        producer = KafkaProducer(
            api_version_auto_timeout_ms=15000,
            bootstrap_servers=params['kafka_brokers_sasl'],
            sasl_plain_username=params['user'],
            sasl_plain_password=params['password'],
            security_protocol=security_protocol,
            ssl_context=context,
            sasl_mechanism=sasl_mechanism)

        print "Created producer"

        # only use the key parameter if it is present
        if 'key' in params:
            producer.send(params['topic'], bytes(params['value']), key=bytes(params['key']))
        else:
            producer.send(params['topic'], bytes(params['value'])

        producer.flush()

        print  "Sent message"
    except NoBrokersAvailable:
        # this exception's message is a little too generic
        return {'error': 'No brokers available. Check that your supplied brokers are correct and available.'}
    except Exception as e:
        return {'error': '{}'.format(e)}

    return {"success": True}

def validateParams(params):
    requiredParams = ['kafka_brokers_sasl', 'user', 'password', 'topic', 'value']
    actualParams = params.keys()

    missingParams = []

    for requiredParam in requiredParams:
        if requiredParam not in params:
            missingParams.append(requiredParam)

    if len(missingParams) > 0:
        return (False, "You must supply all of the following parameters: {}".format(', '.join(missingParams)))
    else:
        return (True, "Params all check out.")
