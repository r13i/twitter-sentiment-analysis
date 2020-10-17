import json
import logging
import requests

from kafka import KafkaProducer
from influxdb import InfluxDBClient
from influxdb.exceptions import InfluxDBServerError, InfluxDBClientError

class TweetsProducer(KafkaProducer):
    '''
    Tweets Producer class inheriting from the KafkaProducer class to
    facilitate connection and interaction with a Kafka broker.

    This class fetches a continuous stream of tweets from Twitter's API
    and sends the text of these tweets into a Kafka topic for further processing.
    
    Also it connects to InfluxDB as time-series database to store some 
    meta-data from these tweets.
    '''
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
            stream = True
        )

        for line in response.iter_lines():
            if line and line != b'Rate limit exceeded':
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

                if geo_data := line.get('includes', {}).get('places'):
                    # If tweet is tagged in a specific location
                    if coordinates := line['data'].get('geo', {}).get('coordinates', {}).get('coordinates'):
                        lon, lat = coordinates
                    # Else use the generic location of the tweet
                    else:
                        lon1, lat1, lon2, lat2 = geo_data[0]['geo']['bbox']
                        lon = (lon1 + lon2) / 2
                        lat = (lat1 + lat2) / 2

                    # Set country_code and place_name as tags so that we can
                    # filter and group by these values
                    data_point[0]["tags"]["country_code"] = geo_data[0]["country_code"]
                    data_point[0]["tags"]["place_name"] = geo_data[0]["full_name"]

                    # Latitudes and Longitudes should be set as fields to be
                    # retrieved with a SELECT statement
                    data_point[0]["fields"]["latitude"] = lat
                    data_point[0]["fields"]["longitude"] = lon

                try:
                    self.influxdb_client.write_points(data_point)
                    logging.info("Successfully stored ID '{}'.".format(line['data']['id']))
                except (InfluxDBServerError, InfluxDBClientError) as e:
                    logging.info("Failed at storing ID '{}'. Error: {}".format(line['data']['id'], e))

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
