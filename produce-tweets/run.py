import os
import json
import logging
import configparser
import requests
from threading import Timer

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


# def stream_connect(url, auth):
#     """
#     Stream a 1% sample from all real-time tweets
#     """
#     response = requests.get(
#         url = url,
#         params = PARAMS,
#         auth = auth,
#         headers = { "User-Agent": "TwitterDevSampledStreamQuickStartPython" },
#         stream = True
#     )

#     languages = {}
#     for line in response.iter_lines():
#         if line:
#             line = json.loads(line)
#             # print(line['data']['text'])

#             if line['data']['lang'] not in languages:
#                 languages[line['data']['lang']] = 0

#             languages[line['data']['lang']] += 1
#             # print(languages)
#             dash(languages)


def connect_broker(broker, topic, retry=3):
    try:
        logging.info("Attempting connection to Kafka topic '{}'@'{}' ...".format(topic, broker))
        tweets_producer = TweetsProducer(
            bootstrap_servers = broker,
            topic = topic
        )

    except NoBrokersAvailable as e:
        logging.warning("No brokers found at '{}'. Attempting reconnect ...".format(broker))

        t = Timer(retry, connect_broker, args=None, kwargs={'broker': broker, 'topic': topic})
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
    broker              = config['kafka'].get('broker')
    topic               = config['kafka'].get('topic')

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
    # Iterate over and over until the broker starts and becomes available
    while (tweets_producer := connect_broker(broker, topic)) is None:
        continue


    logging.info("Starting publishing...")
    try:
        while True:
            # stream_connect(stream_url, bearer_token)
            tweets_producer.produce(stream_url, PARAMS, bearer_token)

    except KeyboardInterrupt:
        tweets_producer.close()
        logging.info("Producer closed. Bye!")
        exit(0)