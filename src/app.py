from urllib.parse import quote
from base64 import b64encode
import json
import requests

API_KEY = "t45mN2Qw6cDTS5T3YBzy1evpy"
API_SECRET = "dhL74vqvuN6LgLSzM2Eesp510Cb7yy3Z1hyUtwXz3KsPPaXkok"

def getAccessToken():
    # Encode the key and secret to the RFC 1738 (i.e. replace special characters with %xx)
    quoted_api_key, quoted_api_secret = quote(API_KEY), quote(API_SECRET)
    bearer_token_credentials = "{}:{}".format(quoted_api_key, quoted_api_secret).encode()
    base64_bearer_token_credentials = b64encode(bearer_token_credentials).decode()

    headers = {
        "Host": "api.twitter.com",
        "Authorization": "Basic {}".format(base64_bearer_token_credentials),
        "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8",
        "Accept-Encoding": "gzip"
    }

    response = requests.post(url="https://api.twitter.com/oauth2/token?grant_type=client_credentials", headers=headers)
    response = json.loads(response.text)

    if response['token_type'] != 'bearer':
        return

    return response['access_token']

def invalidateAccessToken(token):
    
    headers = {
        "Host": "api.twitter.com",
        "Authorization": "Basic {}".format(token),
        "Accept": "*/*",
        "Content-Type": "application/x-www-form-urlencoded"
    }

    r = requests.post(url="https://api.twitter.com/oauth2/invalidate_token?access_token=P{}".format(token), headers=headers)
    print(r.text)



if __name__ == "__main__":

    ACCESS_TOKEN = getAccessToken()

    invalidateAccessToken(ACCESS_TOKEN)

    # headers = {
    #     "Host": "api.twitter.com",
    #     "Authorization": "Bearer {}".format(ACCESS_TOKEN),
    #     "Accept-Encoding": "gzip"
    # }

    # r = requests.get("https://api.twitter.com/1.1/statuses/user_timeline.json?count=100&screen_name=twitterapi")

    # print(r.text)