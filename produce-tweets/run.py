import os
import json
import configparser
import requests

from bearer_token_auth import BearerTokenAuth
from tweets_producer import TweetsProducer
from utils.utils import init_logger, dash


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




if __name__ == "__main__":

    ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
    CONFIG_PATH = os.path.join(ROOT_DIR, 'config.ini')
    SECRET_PATH = os.path.join(ROOT_DIR, 'secret.ini')

    logger = init_logger()

    config = configparser.ConfigParser(strict=True)
    config.read_file(open(CONFIG_PATH, 'r'))

    bearer_token_url    = config['twitter'].get('bearer_token_url').encode()
    stream_url          = config['twitter'].get('stream_url').encode()
    kafka_broker        = config['kafka'].get('broker')
    kafka_topic         = config['kafka'].get('topic')

    try:
        config.read(SECRET_PATH)
        consumer_key = config['twitter'].get('key').encode()
        consumer_secret = config['twitter'].get('secret').encode()
        logger.info("Twitter API credentials parsed.")
    except KeyError as e:
        logger.error("Secret file not found. Make sure it is available in the directory.")
        exit()
    except AttributeError as e:
        logger.error("Cannot read Twitter API credentials. Make sure that API key and secret are in the secret file (also check spelling).")
        exit()

    bearer_token = BearerTokenAuth(bearer_token_url, consumer_key, consumer_secret)

    logger.info("Connecting to Kafka topic '{}'@'{}' ...".format(kafka_topic, kafka_broker)
    tweets_producer = TweetsProducer(
        broker = kafka_broker,
        topic = kafka_topic,
        logger = logger
    )

    logger.info("Starting publishing.")
    while True:
        try:
            # stream_connect(stream_url, bearer_token)
            tweets_producer.produce(stream_url, PARAMS, bearer_token)
        except KeyboardInterrupt:
            tweets_producer.close()
            logger.info("Producer closed. Bye!")
            exit(0)