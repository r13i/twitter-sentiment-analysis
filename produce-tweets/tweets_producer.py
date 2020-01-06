import json
import requests
import kafka

from pprint import pprint

class TweetsProducer(object):
    def __init__(self, broker, topic, logger):
        self.broker = broker
        self.topic = topic

        try:
            self.producer = kafka.KafkaProducer(bootstrap_servers=self.broker)
        except kafka.errors.NoBrokersAvailable as e:
            logger.error("No brokers found at '{}'.".format(self.broker))
            exit()

    def produce(self, stream_url, params, auth):
        """
        Stream a 1% sample from all real-time tweets
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