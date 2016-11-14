import logging
import time

from consumer import Consumer
from consumercollection import ConsumerCollection
from threading import Thread

class TheDoctor (Thread):
    sleepy_time = 2

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

            time.sleep(self.sleepy_time)
