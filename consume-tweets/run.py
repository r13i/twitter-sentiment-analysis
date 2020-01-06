import os
import configparser
from kafka import KafkaConsumer

if __name__ == "__main__":

    ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
    CONFIG_PATH = os.path.join(ROOT_DIR, 'config.ini')

    config = configparser.ConfigParser(strict=True)
    config.read_file(open(CONFIG_PATH, 'r'))

    consumer = KafkaConsumer(
        config['kafka'].get('topic'),
        bootstrap_servers=config['kafka'].get('broker'))

    while True:
        msg = next(consumer)

        print(msg)