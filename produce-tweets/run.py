import os
import json
import logging
import configparser
import requests
from threading import Timer
from requests.exceptions import ConnectionError

from kafka.errors import NoBrokersAvailable

from bearer_token_auth import BearerTokenAuth
from tweets_producer import TweetsProducer
from utils.utils import dash


PARAMS = {
    # "format": "detailed",
    "tweet.format": "detailed",
    # "user.format": "detailed",
    # "place.format": "detailed",
    # "expansions": "attachments.poll_ids,attachments.media_keys,author_id,entities.mentions.username,geo.place_id,in_reply_to_user_id,referenced_tweets.id,referenced_tweets.id.author_id"
}

def connect_broker(broker, topic,
    influxdb_host, influxdb_port, influxdb_database, interval_sec=3):

    try:
        logging.info("Attempting connection to Kafka topic '{}'@'{}' ...".format(topic, broker))
        tweets_producer = TweetsProducer(
            bootstrap_servers = broker,
            topic = topic,
            influxdb_host = influxdb_host,
            influxdb_port = influxdb_port,
            influxdb_database = influxdb_database)

    except NoBrokersAvailable as e:
        logging.warning("No brokers found at '{}'. Attempting reconnect ...".format(broker))

        t = Timer(interval_sec, connect_broker, args=None, kwargs={'broker': broker, 'topic': topic})
        t.start()

    else:
        return tweets_producer

if __name__ == "__main__":
    # Load-up config file
    ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
    CONFIG_PATH = os.path.join(ROOT_DIR, 'config.ini')
    SECRET_PATH = os.path.join(ROOT_DIR, 'secret.ini')

    config = configparser.ConfigParser(strict=True)
    config.read_file(open(CONFIG_PATH, 'r'))

    # Setup logging
    logging.basicConfig(
        level = logging.INFO,
        format = "[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s")

    # Read config paramaters
    bearer_token_url    = config['twitter'].get('bearer_token_url').encode()
    stream_url          = config['twitter'].get('stream_url').encode()

    try:
        config.read(SECRET_PATH)
        consumer_key = config['twitter'].get('key').encode()
        consumer_secret = config['twitter'].get('secret').encode()
        logging.info("Twitter API credentials parsed.")
    except KeyError as e:
        logging.error("Secret file not found. Make sure it is available in the directory.")
        exit()
    except AttributeError as e:
        logging.error("Cannot read Twitter API credentials. Make sure that API key and secret are in the secret file (also check spelling).")
        exit()

    # Access Twitter's auth API to obtain a bearer token
    bearer_token = BearerTokenAuth(bearer_token_url, consumer_key, consumer_secret)

    # Attempt connection to Kafka broker
    # Iterate over and over (with a few seconds of interval) until 
    # the broker starts and becomes available
    while (tweets_producer := connect_broker(
        broker              = config['kafka'].get('broker'),
        topic               = config['kafka'].get('topic'),
        influxdb_host       = config['influxdb'].get('host'),
        influxdb_port       = config['influxdb'].get('port'),
        influxdb_database   = config['influxdb'].get('tweets-database'))
    ) is None:
        continue


    logging.info("Starting publishing...")
    while True:
        try:
            tweets_producer.produce(stream_url, PARAMS, bearer_token)

        except requests.exceptions.ChunkedEncodingError as e:
            logging.warning("Connection to Twitter API got broken. Continuing ...")

        except ConnectionError as e:
            logging.warning("Unable to connect to InfluxDB. Continuing ...")

        except KeyboardInterrupt:
            tweets_producer.close()
            logging.info("Producer closed. Bye!")
            exit(0)