import logging

from kafka import KafkaConsumer

class StreamProcess(KafkaConsumer):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.broker = kwargs['bootstrap_servers']

    def process(self):
        try:
            message = self.__next__()
            tweet = message.value.decode('utf-8').strip()

            print(tweet)
            print('=' * 50)
            # logging.info(message.offset)

        except StopIteration as e:
            logging.warning("No incoming message found at Kafka broker: {}.".format(self.broker))
            return