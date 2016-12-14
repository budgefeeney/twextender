"""
This has all the code necessary to download tweets from twitter for a given user up to
a given date
"""
import time
import tweepy
import json
from datetime import datetime, timedelta
from . import tweet

# Consumer keys and access tokens, used for OAuth. This is a dictionary
# with the string keys "consumer", "consumer_secret", "access_token" and
# "access_token_secret"
with open('/Users/bryanfeeney/twitter.budge.key', 'r') as f:
    keys = json.load(f)


# OAuth process, using the keys and tokens
auth = tweepy.OAuthHandler(keys['consumer'], keys['consumer_secret'])
auth.set_access_token(keys['access_token'], keys['access_token_secret'])

# Creation of the actual interface, using authentication
twitter = tweepy.API(auth)

def limit_handled(cursor):
    while True:
        try:
            yield cursor.next()
        except tweepy.RateLimitError:
            time.sleep(15 * 60)


def tweets_for_user(screen_name, max_id, min_date):
    """
    Gets as many tweets as possible for a user, starting from the max_id, and continuing up
    till the since_date, in short blocks, so we don't blow our RateLimit budget.

    Return a tuple of the tweets and a boolean to indicate if more tweets are available (true)
    or not (false)

    :param screen_name: the screen-name of the user whose tweets we want
    :param max_id: we gather all tweets occurring before the tweet with this <code>max_id</code>
    :param min_date: we gather all tweets occurring after this date. This is a <code>datetime<code>
    object
    :return: a tuple: first the list of downloaded tweets, in an arbitrary order; second a boolean
    indicating if more tweets are available to download (true) or not (false)
    """
    result = []
    for status in limit_handled(tweepy.Cursor(twitter.user_timeline, screen_name=screen_name, max_id=max_id).items()):
        result.append(status_to_tweet(status))
        if status.created_at < min_date:
            return result
    return result


def status_to_tweet(status):
    """
    Converts a tweepy.Status object to a tweet.Tweet object. The local_time field is not reliable.
    """
    offset_seconds = 0 if status.user.utc_offset is None else status.user.utc_offset
    local_date = status.created_at + timedelta(seconds=offset_seconds)
    return tweet.TweetEnvelope(
        local_date=local_date,
        utc_date=status.created_at,
        tweet=status.TweetBody(
            tweet_id=status.id,
            author=status.author.screen_name,
            content=status.text,
            embedded_tweet=None,
            embedded_url=None
        )
    )

def print_my_followers_screen_names():
    for follower in limit_handled(tweepy.Cursor(twitter.followers).items()):
        if follower.friends_count < 300:
            print (follower.screen_name)