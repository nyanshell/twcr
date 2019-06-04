import os
import re
import time
import base64
from random import shuffle

import requests
from pymongo import MongoClient
from requests.exceptions import HTTPError
from pymongo.errors import BulkWriteError, DuplicateKeyError


CONSUMER_KEY = os.getenv('CONSUMER_KEY')
CONSUMER_SECRET = os.getenv('CONSUMER_SECRET')
API = "https://api.twitter.com"
session = requests.Session()
client = MongoClient('localhost', 27017)
time.sleep(5.0)
tweets_coll = client['tweets']['tweets']
user_coll = client['users']['users']


def obtain_access_token():
    resp = session.post(
        API + '/oauth2/token',
        headers={
            'Authorization': 'Basic ' + base64.b64encode(
                f'{CONSUMER_KEY}:{CONSUMER_SECRET}'.encode('ascii')).decode('utf-8')},
        data={'grant_type': 'client_credentials'},
        timeout=3.0,
    )
    resp.raise_for_status()
    return resp.json()['access_token']


def get_user_tweets(screen_name, count=20):
    token = obtain_access_token()
    resp = session.get(API + '/1.1/statuses/user_timeline.json',
                       headers={'Authorization': 'Bearer ' + token},
                       params={'count': count, 'screen_name':  screen_name},
                       timeout=10)
    resp.raise_for_status()
    return resp.json()


def get_user(user_id=None, screen_name=None):
    assert user_id or screen_name
    token = obtain_access_token()
    resp = session.get(API + '/1.1/users/show.json',
                       headers={'Authorization': 'Bearer ' + token},
                       params={'user_id':  user_id} if user_id else {'screen_name':  screen_name},
                       timeout=10)
    resp.raise_for_status()
    return resp.json()


def get_user_follower_ids(user_id=None, screen_name=None):
    assert user_id or screen_name
    token = obtain_access_token()
    resp = session.get(API + '/1.1/followers/ids.json',
                       headers={'Authorization': 'Bearer ' + token},
                       params={'user_id':  user_id} if user_id else {'screen_name':  screen_name},
                       timeout=10)
    resp.raise_for_status()
    return resp.json()


def is_zh_tweet(s):
    r = re.findall('[\u4e00-\u9fff]+', s)
    if len(''.join(r)) / len(s) > 0.3:
        return True
    return False


def crawl():
    for user in user_coll.aggregate([{"$sample": {"size": 1000}}]):
        if user['protected']:
            continue
        try:
            tweets = get_user_tweets(user['screen_name'], count=200)
            print('got', len(tweets), 'tweets from', user['screen_name'])
            followers = get_user_follower_ids(screen_name=user['screen_name'])
        except HTTPError:
            print('rate limited while fetching tweets, sleep a while')
            time.sleep(300)
            continue
        try:
            tweets_coll.insert_many(tweets, ordered=False)
        except BulkWriteError:
            pass
        except Exception as err:
            print(err)
        # add new user
        follower_ids = followers['ids']
        shuffle(follower_ids)
        print('got', len(follower_ids), 'followers')
        for uids in follower_ids[:10]:
            try:
                candidate = get_user(user_id=uids)
                if not user['protected']:
                    tweets = get_user_tweets(candidate['screen_name'], count=200)
                    print('got', len(tweets), 'tweets from', candidate['screen_name'])
                    if sum(is_zh_tweet(t['text']) for t in tweets) > 0.2:
                        user_coll.insert_one(candidate)
                        tweets_coll.insert_many(tweets, ordered=False)
            except DuplicateKeyError:
                pass
            except BulkWriteError:
                pass
            except HTTPError:
                print('rate limited, stop fetching users')
                break
            except Exception as err:
                print(err)


if __name__ == '__main__':
    while True:
        try:
            crawl()
        except Exception as err:
            print(err)
            time.sleep(300)
