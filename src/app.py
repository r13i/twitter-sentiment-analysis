import json
import requests

from pprint import pprint

CONSUMER_KEY = "t45mN2Qw6cDTS5T3YBzy1evpy"
CONSUMER_SECRET = "dhL74vqvuN6LgLSzM2Eesp510Cb7yy3Z1hyUtwXz3KsPPaXkok"
STREAM_URL = "https://api.twitter.com/labs/1/tweets/stream/sample"

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
        auth = auth,
        headers = { "User-Agent": "TwitterDevSampledStreamQuickStartPython" },
        stream = True
    )

    for line in response.iter_lines():
        if line:
            line = json.loads(line)
            print(line['data']['text'])




if __name__ == "__main__":

    bearer_token = BearerTokenAuth(CONSUMER_KEY, CONSUMER_SECRET)

    while True:
        stream_connect(STREAM_URL, bearer_token)