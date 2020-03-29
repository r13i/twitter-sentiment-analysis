import json
import logging
import re, string
import pickle
from requests.exceptions import ConnectionError

from kafka import KafkaConsumer
from nltk.tag import pos_tag
from nltk.corpus import stopwords
from nltk.stem.wordnet import WordNetLemmatizer
from nltk.tokenize import TweetTokenizer
from influxdb import InfluxDBClient

class StreamProcess(KafkaConsumer):
    def __init__(self, *args, **kwargs):
        self.broker                 = kwargs['bootstrap_servers']
        self._classifier_filepath   = kwargs.pop('classifier_filepath', None)
        self.influxdb_host          = kwargs.pop('influxdb_host', 'localhost')
        self.influxdb_port          = kwargs.pop('influxdb_port', 8086)
        self.influxdb_database      = kwargs.pop('influxdb_database', None)

        super().__init__(*args, **kwargs)

        self._stopwords = stopwords.words('english')

        with open(self._classifier_filepath, 'rb') as f:
            self._classifier = pickle.load(f)

        self._word_tokenizer = TweetTokenizer(
            preserve_case=True,
            reduce_len=False,
            strip_handles=False)

        self._lemmatizer = WordNetLemmatizer()

        self.influxdb_client = InfluxDBClient(
            host        = self.influxdb_host,
            port        = self.influxdb_port,
            username    = 'root',
            password    = 'root',
            database    = self.influxdb_database)
        self.influxdb_client.create_database(self.influxdb_database)

    def process(self):
        try:
            message = self.__next__()
            message = json.loads(message.value.decode('utf-8'))

            id = message['id']
            tweet = message['tweet'].strip()

            # Infer sentiment from tweet
            polarity = self._classify(tweet)

            data_point = [{
                # "timestamp":
                "measurement": self.influxdb_database,
                "tags": {
                    "language": "en",
                    "polarity": polarity
                },
                "fields": {
                    "id": id
                }
            }]

            if self.influxdb_client.write_points(data_point):
                logging.info("DB SUCCESSFUL")
            else:
                logging.info("DB FAILED")

            # logging.info(message.offset)

        except StopIteration as e:
            logging.warning("No incoming message found at Kafka broker: {}.".format(self.broker))
            return
        
        except ConnectionError as e:
            logging.warning("Unable to connect to InfluxDB. Continuing ...")
            return
            

    def _tokenize(self, tweet):
        return self._word_tokenizer.tokenize(tweet)

    def _is_noise(self, word):
        pattern = 'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+#]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+|(@[A-Za-z0-9_]+)'
        return word in string.punctuation \
            or word.lower() in self._stopwords \
            or re.search(pattern, word, re.IGNORECASE) != None

    def _tag2type(self, tag):
        """
        Take a tag and return a type
        Common tags are:
            - NNP: Noun, proper, singular
            - NN: Noun, common, singular or mass
            - IN: Preposition or conjunction, subordinating
            - VBG: Verb, gerund or present participle
            - VBN: Verb, past participle

        return 'n' for noun, 'v' for verb, and 'a' for any
        """
        if tag.startswith('NN'):
            return 'n'
        elif tag.startswith('VB'):
            return 'v'
        else:
            return 'a'

    def _lemmatize(self, tokens):
        return [
            self._lemmatizer.lemmatize(word, self._tag2type(tag)).lower()
            for word, tag in pos_tag(tokens) if not self._is_noise(word)
        ]

    def _classify(self, tweet):
        tokens = self._lemmatize(self._tokenize(tweet))
        return self._classifier.classify(dict([token, True] for token in tokens))