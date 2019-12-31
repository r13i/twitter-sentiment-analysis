import requests

class BearerTokenAuth(requests.auth.AuthBase):
    """
    Request an OAuth2 Bearer Token from Twitter
    """
    def __init__(self, bearer_token_url, consumer_key, consumer_secret):
        self.bearer_token_url = bearer_token_url
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
