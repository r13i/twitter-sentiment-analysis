import json
import requests

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

class TweetsProducer(object):
    def __init__(self, broker, topic, logger, **kwargs):
        self.broker = broker
        self.topic = topic
        self.logger = logger

        try:
            self.producer = KafkaProducer(
                bootstrap_servers = self.broker
            )
        except NoBrokersAvailable as e:
            self.logger.error("No brokers found at '{}'.".format(self.broker))
            exit()

    def produce(self, stream_url, params, auth):
        """
        Stream a 1% sample from worldwide real-time tweets
        See Twitter Labs sample-stream docs for more details
        """
        response = requests.get(
            url = stream_url,
            params = params,
            auth = auth,
            # headers = { "User-Agent": "TwitterDevSampledStreamQuickStartPython" },
            stream = True
        )

        for line in response.iter_lines():
            if line:
                line = json.loads(line)
                
                if line['data']['lang'] == 'en':
                    self.producer.send(self.topic, line['data']['text'].encode())
                    # print(self.producer.metrics())

    def close(self):
        self.producer.close()