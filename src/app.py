import json
import requests
from pprint import pprint

from utils.utils import dash


CONSUMER_KEY = "t45mN2Qw6cDTS5T3YBzy1evpy"
CONSUMER_SECRET = "dhL74vqvuN6LgLSzM2Eesp510Cb7yy3Z1hyUtwXz3KsPPaXkok"
STREAM_URL = "https://api.twitter.com/labs/1/tweets/stream/sample"
PARAMS = {
    # "format": "detailed",
    "tweet.format": "detailed",
    # "user.format": "detailed",
    # "place.format": "detailed",
    # "expansions": "attachments.poll_ids,attachments.media_keys,author_id,entities.mentions.username,geo.place_id,in_reply_to_user_id,referenced_tweets.id,referenced_tweets.id.author_id"
}

class BearerTokenAuth(requests.auth.AuthBase):
    """
    Request an OAuth2 Bearer Token from Twitter
    """
    def __init__(self, consumer_key, consumer_secret):
        self.bearer_token_url = "https://api.twitter.com/oauth2/token"
        self.consumer_key = consumer_key
        self.consumer_secret = consumer_secret
        self.bearer_token = self.get_bearer_token()

    def get_bearer_token(self):
        response = requests.post(
            url = self.bearer_token_url,
            auth = (self.consumer_key, self.consumer_secret),
            data = { "grant_type": "client_credentials" },
            headers = { "User-Agent": "TwitterDevSampledStreamQuickStartPython" }
        )

        if response.status_code is not 200:
            raise Exception("Cannot get a bearer token (HTTP {}): {}"
                .format(response.status_code, response.text))

        body = response.json()
        return body['access_token']

    def __call__(self, r):
        r.headers['Authorization'] = "Bearer {}".format(self.bearer_token)
        return r


def stream_connect(url, auth):
    """
    Stream a 1% sample from all real-time tweets
    """
    response = requests.get(
        url = url,
        params = PARAMS,
        auth = auth,
        headers = { "User-Agent": "TwitterDevSampledStreamQuickStartPython" },
        stream = True
    )

    languages = {}
    for line in response.iter_lines():
        if line:
            line = json.loads(line)
            # print(line['data']['text'])

            if line['data']['lang'] not in languages:
                languages[line['data']['lang']] = 0

            languages[line['data']['lang']] += 1
            # print(languages)
            dash(languages)




if __name__ == "__main__":

    bearer_token = BearerTokenAuth(CONSUMER_KEY, CONSUMER_SECRET)

    while True:
        stream_connect(STREAM_URL, bearer_token)