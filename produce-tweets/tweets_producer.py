import json
import logging
import requests

from kafka import KafkaProducer
from influxdb import InfluxDBClient


from pprint import pprint

class TweetsProducer(KafkaProducer):
    def __init__(self, topic, *args, **kwargs):
        self.topic = topic

        self.influxdb_host          = kwargs.pop('influxdb_host', 'localhost')
        self.influxdb_port          = kwargs.pop('influxdb_port', 8086)
        self.influxdb_database      = kwargs.pop('influxdb_database', None)
        self.influxdb_client = InfluxDBClient(
            host        = self.influxdb_host,
            port        = self.influxdb_port,
            username    = 'root',
            password    = 'root',
            database    = self.influxdb_database)
        self.influxdb_client.create_database(self.influxdb_database)

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

                # Storing tweets' language
                data_point = [{
                    # "timestamp":
                    "measurement": self.influxdb_database,
                    "tags": {
                        "language": line['data']['lang'],
                    },
                    "fields": {
                        "id": line['data']['id']
                    }
                }]

                if self.influxdb_client.write_points(data_point):
                    logging.info("Successfully stored ID '{}'.".format(line['data']['id']))
                else:
                    logging.info("Failed at storing ID '{}'.".format(line['data']['id']))

                # Queueing tweets into Kafka for further processing
                if line['data']['lang'] == 'en':
                    self.send(
                        self.topic,
                        json.dumps({
                            'id': line['data']['id'],
                            'tweet': line['data']['text']
                        }).encode())

                    logging.info("Queued tweet '{}'.".format(line['data']['id']))
                    # logging.info(self.metrics())
