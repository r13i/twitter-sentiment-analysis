import os
import logging
import configparser
from threading import Timer

from kafka.errors import NoBrokersAvailable

from stream_process import StreamProcess


def connect_broker(broker, topic, classifier_filepath,
    influxdb_host, influxdb_port, influxdb_database, interval_sec=3):

    try:
        logging.info("Attempting connection to Kafka topic '{}'@'{}' ...".format(topic, broker))

        consumer = StreamProcess(
            topic,   # Kafka topic
            classifier_filepath = classifier_filepath,
            bootstrap_servers = broker,
            enable_auto_commit = True,
            auto_offset_reset = 'latest',
            influxdb_host = influxdb_host,
            influxdb_port = influxdb_port,
            influxdb_database = influxdb_database)

    except NoBrokersAvailable as e:
        logging.warning("No brokers found at '{}'. Attempting reconnect ...".format(broker))

        t = Timer(interval_sec, connect_broker, args=None, kwargs={'broker': broker, 'topic': topic})
        t.start()

    else:
        return consumer

if __name__ == "__main__":

    # Load-up config file
    ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
    CONFIG_PATH = os.path.join(ROOT_DIR, 'config.ini')

    config = configparser.ConfigParser(strict=True)
    config.read_file(open(CONFIG_PATH, 'r'))

    # Setup logging
    logging.basicConfig(
        level = logging.INFO,
        format = "[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s")

    # Attempt connection to Kafka broker
    # Iterate over and over (with a few seconds of interval) until 
    # the broker starts and becomes available
    while (consumer := connect_broker(
        broker              = config['kafka'].get('broker'),
        topic               = config['kafka'].get('topic'),
        classifier_filepath = config['classifier'].get('path'),
        influxdb_host       = config['influxdb'].get('host'),
        influxdb_port       = config['influxdb'].get('port'),
        influxdb_database   = config['influxdb'].get('sentiments-database'))
    ) is None:
        continue

        # consumer = StreamProcess(
        #     'tweets',   # Kafka topic
        #     classifier_filepath = './consume-tweets/model.pickle',
        #     bootstrap_servers = 'localhost:9092',
        #     enable_auto_commit = True,
        #     auto_offset_reset = 'latest',
        #     influxdb_host = 'localhost',
        #     influxdb_port = '8086',
        #     influxdb_database = 'tweets')

    try:
        while True:
            consumer.process()

    except KeyboardInterrupt:
        consumer.close()
        logging.info("Consumer closed. Bye!")
        exit(0)