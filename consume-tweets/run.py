import os
import logging
import configparser
from threading import Timer
from requests.exceptions import ConnectionError

from kafka.errors import NoBrokersAvailable

from tweets_consumer import TweetsConsumer


def connect_broker(broker, topic, classifier_filepath,
    influxdb_host, influxdb_port, influxdb_database, interval_sec=3):

    '''
    Helper function that tries to (re)connect to a Kafka broker
    until a connection is established.

    @arg broker String representing the hostname and port of the Kafka broker, e.g. 'kafka:9093'.
    @arg topic String representing the topic name, e.g. tweets.
    @arg classifier_filepath String representing path to the ML classifier pickle file.
    @arg influxdb_host String representing the hostname of InfluxDB, e.g. 'influxdb'.
    @arg influxdb_port Int representing the port of InfluxDB, e.g. 8086.
    @arg influxdb_database String representing the name of the InfluxDB database, e.g. 'tweets'.
    @arg interval_sec Int (Default 3) representing the number of seconds before attempting a reconnect.

    @return Instance of TweetsConsumer.
    '''

    try:
        logging.info("Attempting connection to Kafka topic '{}'@'{}' ...".format(topic, broker))

        consumer = TweetsConsumer(
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
    # Poll over and over (with a few seconds of interval) until 
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

    # Main loop
    while True:
        try:
            consumer.process()

        except ConnectionError as e:
            logging.warning("Unable to connect to InfluxDB. Continuing ...")

        except KeyboardInterrupt:
            consumer.close()
            logging.info("Consumer closed. Bye!")
            exit(0)