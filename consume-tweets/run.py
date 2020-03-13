import os
import logging
import configparser

from kafka.errors import NoBrokersAvailable

from stream_process import StreamProcess

if __name__ == "__main__":

    # Load-up config file
    ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
    CONFIG_PATH = os.path.join(ROOT_DIR, 'config.ini')

    # config = configparser.ConfigParser(strict=True)
    # config.read_file(open(CONFIG_PATH, 'r'))

    # Setup logging
    logging.basicConfig(
        level = logging.INFO,
        format = "[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s")

    try:
        # logging.info("Connecting to Kafka topic '{}'@'{}'"
        #     .format(config['kafka'].get('topic'), config['kafka'].get('broker')))

        # consumer = StreamProcess(
        #     config['kafka'].get('topic'),
        #     bootstrap_servers = config['kafka'].get('broker'),
        #     enable_auto_commit = True,
        #     auto_offset_reset = 'latest')

        consumer = StreamProcess(
            'tweets',   # Kafka topic
            classifier_filepath = './consume-tweets/model.pickle',
            bootstrap_servers = 'localhost:9092',
            enable_auto_commit = True,
            auto_offset_reset = 'latest')

    except NoBrokersAvailable as e:
        logging.error("No brokers found at '{}'.".format(config['kafka'].get('broker')))
        exit()


    try:
        while True:
            consumer.process()

    except KeyboardInterrupt:
        consumer.close()
        logging.info("Consumer closed. Bye!")
        exit(0)