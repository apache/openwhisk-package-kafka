import logging
import time

from consumer import Consumer
from threading import Thread

class TheDoctor (Thread):
    # maximum time to allow a consumer to not successfully poll() before restarting
    poll_timeout_seconds = 20

    # interval between the Doctor making rounds
    sleepy_time_seconds = 2

    def __init__(self, consumerCollection):
        Thread.__init__(self)

        self.daemon = True
        self.consumerCollection = consumerCollection

    def run(self):
        logging.info('[Doctor] The Doctor is in!')

        while True:
            consumers = self.consumerCollection.getCopyForRead()

            for consumerId in consumers:
                consumer = consumers[consumerId]
                logging.debug('[Doctor] [{}] Consumer is in state: {}'.format(consumerId, consumer.currentState()))

                if consumer.currentState() is Consumer.State.Dead and consumer.desiredState() is Consumer.State.Running:
                    # well this is unexpected...
                    logging.error('[Doctor][{}] Consumer is dead, but should be alive!'.format(consumerId))
                    consumer.restart()
                elif consumer.currentState() is Consumer.State.Dead and consumer.desiredState() is Consumer.State.Dead:
                    # Bring out yer dead...
                    logging.info('[{}] Removing dead consumer from the collection.'.format(consumer.trigger))
                    self.consumerCollection.removeConsumerForTrigger(consumer.trigger)
                elif consumer.secondsSinceLastPoll() > self.poll_timeout_seconds and consumer.desiredState() is Consumer.State.Running:
                    # there seems to be an issue with the kafka-python client where it gets into an
                    # error-handling loop. This causes poll() to never complete, but also does not
                    # throw an exception.
                    logging.error('[Doctor][{}] Consumer timed-out, but should be alive! Restarting consumer.'.format(consumerId))
                    consumer.restart()

            time.sleep(self.sleepy_time_seconds)
