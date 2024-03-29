import os
import time
import base64
from random import shuffle

import requests
from langdetect import detect as detect_lang
from pymongo import MongoClient
from redis import from_url
from requests.exceptions import HTTPError
from pymongo.errors import BulkWriteError, DuplicateKeyError
from langdetect.lang_detect_exception import LangDetectException


CONSUMER_KEY = os.getenv('CONSUMER_KEY')
CONSUMER_SECRET = os.getenv('CONSUMER_SECRET')
MONGODB_URL = os.getenv('MONGODB_URL', 'localhost')
API = "https://api.twitter.com"
session = requests.Session()
client = MongoClient(MONGODB_URL, 27017)
time.sleep(5.0)
tweets_coll = client['tweets']['tweets']
user_coll = client['users']['users']
redis_conn = from_url(os.getenv('REDIS_URL', 'redis://localhost:6379'))
ZH_TWEET_THRESHOLD = 0.3
ZH_USER_SEEDS = os.getenv('SEED_USERS', 'zh_users_list.txt')
REDIS_USERS = 'twitter:zh_users'


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


def count_zh_tweets(tweets):
    cnt = 0
    for t in tweets:
        try:
            if detect_lang(t['text']).startswith('zh'):
                cnt += 1
        except LangDetectException:
            pass
    return cnt


def get_zh_users():
    while True:
        user = redis_conn.spop(REDIS_USERS).decode('utf-8')
        yield {'protected': False, 'screen_name': user}


def crawl():
    for user in get_zh_users():
        try:
            tweets = get_user_tweets(user['screen_name'], count=200)
            print('got', len(tweets), 'tweets from', user['screen_name'])
            zh_tweets_cnt = count_zh_tweets(tweets)
            if zh_tweets_cnt / len(tweets) > ZH_TWEET_THRESHOLD:
                redis_conn.sadd(REDIS_USERS, user['screen_name'].encode('utf-8'))
                try:
                    tweets_coll.insert_many(tweets, ordered=False)
                except BulkWriteError as err:
                    print(err)
                    print('ignoring...')
            else:
                print(user['screen_name'], 'not a zh user')
                continue
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
                    if not tweets:
                        print('no tweets from', candidate['screen_name'], 'ignore.')
                        continue
                    zh_tweets_cnt = count_zh_tweets(tweets)
                    print('zh tweets:', zh_tweets_cnt, 'current batch:',
                          len(tweets), 'zh rate:', zh_tweets_cnt / len(tweets))
                    if zh_tweets_cnt / len(tweets) > ZH_TWEET_THRESHOLD:
                        redis_conn.sadd(REDIS_USERS, candidate['screen_name'].encode('utf-8'))
                        user_coll.insert_one(candidate)
                        tweets_coll.insert_many(tweets, ordered=False)
                    else:
                        print(candidate['screen_name'], 'not a zh user')
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
