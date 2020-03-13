import json
import logging
import requests

from kafka import KafkaProducer

class TweetsProducer(KafkaProducer):
    def __init__(self, topic, *args, **kwargs):
        self.topic = topic
        super().__init__(*args, **kwargs)

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
                    self.send(self.topic, line['data']['text'].encode())
                    logging.info(line['data']['text'])
                    # print(self.metrics())
