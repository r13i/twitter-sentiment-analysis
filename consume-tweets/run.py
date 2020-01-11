import os
import logging
import configparser

from kafka import KafkaConsumer

if __name__ == "__main__":

    logging.basicConfig(
        level = logging.INFO,
        format = "[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s")

    ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
    CONFIG_PATH = os.path.join(ROOT_DIR, 'config.ini')

    config = configparser.ConfigParser(strict=True)
    config.read_file(open(CONFIG_PATH, 'r'))

    consumer = KafkaConsumer(
        config['kafka'].get('topic'),
        bootstrap_servers = config['kafka'].get('broker'),
        enable_auto_commit = True,
        auto_offset_reset = 'latest')

    # consumer = KafkaConsumer(
    #     'tweets',
    #     bootstrap_servers = 'localhost:9092',
    #     enable_auto_commit = True,
    #     auto_offset_reset = 'latest')


    while True:
        try:
            msg = next(consumer)
            print(msg.value.decode("utf-8") )
            logging.info(msg.offset)
        except KeyboardInterrupt:
            consumer.close()
            logging.info("Consumer closed. Bye!")
            exit(0)