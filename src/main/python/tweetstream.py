import oauth2
import urllib2
import json
from keys import access_token_key, access_token_secret, api_key, api_secret

_debug = 0

oauth_token = oauth2.Token(key=access_token_key, secret=access_token_secret)
oauth_consumer = oauth2.Consumer(key=api_key, secret=api_secret)

signature_method_hmac_sha1 = oauth2.SignatureMethod_HMAC_SHA1()

http_method = "GET"

http_handler = urllib2.HTTPHandler(debuglevel=_debug)
https_handler = urllib2.HTTPSHandler(debuglevel=_debug)


def twitter_request(url, parameters):
    req = oauth2.Request.from_consumer_and_token(oauth_consumer,
                                                 token=oauth_token,
                                                 http_method=http_method,
                                                 http_url=url,
                                                 parameters=parameters)

    req.sign_request(signature_method_hmac_sha1, oauth_consumer, oauth_token)

    url = req.to_url()

    opener = urllib2.OpenerDirector()
    opener.add_handler(http_handler)
    opener.add_handler(https_handler)

    response = opener.open(url, None)

    return response


def fetchsamples(num_samples=100000):
    url = "https://stream.twitter.com/1.1/statuses/sample.json"
    response = twitter_request(url, [])
    count = 0
    for line in response:
        try:
            j = json.loads(line.strip())
        except ValueError:
            continue
        if 'text' in j and is_ascii(j['text']) and 'retweeted' in j and not j['retweeted']:
            print j['id'], j['text'].encode('utf-8')
            count += 1
            if count >= num_samples:
                break


def is_ascii(s):
    return all(ord(c) < 128 for c in s)


fetchsamples(1000)
