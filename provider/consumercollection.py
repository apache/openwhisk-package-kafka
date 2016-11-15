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
