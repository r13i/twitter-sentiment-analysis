import logging
import re, string

from kafka import KafkaConsumer
from nltk.tag import pos_tag
from nltk.corpus import stopwords
from nltk.stem.wordnet import WordNetLemmatizer
from nltk.tokenize import TweetTokenizer

class StreamProcess(KafkaConsumer):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.broker = kwargs['bootstrap_servers']
        self.stopwords = stopwords.words('english')

        self._word_tokenizer = TweetTokenizer(
            preserve_case=True,
            reduce_len=False,
            strip_handles=False)

        self._lemmatizer = WordNetLemmatizer()

    def process(self):
        try:
            message = self.__next__()
            tweet = message.value.decode('utf-8').strip()

            print(tweet)
            print(self._lemmatize(self._tokenize(tweet)))
            print('=' * 50)
            # logging.info(message.offset)

        except StopIteration as e:
            logging.warning("No incoming message found at Kafka broker: {}.".format(self.broker))
            return

    def _tokenize(self, t):
        return self._word_tokenizer.tokenize(t)

    def _is_noise(self, word):
        pattern = 'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+#]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+|(@[A-Za-z0-9_]+)'
        return word in string.punctuation \
            or word.lower() in self.stopwords \
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