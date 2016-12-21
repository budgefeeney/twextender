"""
This has all the code necessary to download tweets from twitter for a given user up to
a given date
"""
import time
import tweepy
import json
from datetime import timedelta
import re
from . import tweet

LastTwitterLink = re.compile("\\s*https?://t\\.co/[A-Za-z0-9]{8,12}\\s*$", re.IGNORECASE)

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
            print (" *** Paused, rate-limit exceeded")
            time.sleep(15 * 60)
        except tweepy.TweepError as e:
            if e.response.status_code == 429:
                print (" *** Paused, rate-limit exceeded")
                time.sleep(15 * 60)
            else:
                raise e


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

    for status in limit_handled(tweepy.Cursor(twitter.user_timeline, screen_name=screen_name, exclude_replies=True, tweet_mode="extended", max_id=max_id).items()):
        result.append(status_to_tweet(status))
        if status.created_at < min_date:
            return result

    if len(result) > 0 and result[0].tweet.tweet_id == max_id:
        del result[0]
    return result


def status_to_tweet(status):
    """
    Converts a tweepy.Status object to a tweet.Tweet object. The local_time field is not reliable.
    """
    # TODO Figure out the timezone stuff
    offset_seconds = 0 if status.user.utc_offset is None else status.user.utc_offset
    local_date = status.created_at + timedelta(seconds=offset_seconds)

    if hasattr (status, 'retweeted_status'):
        rstatus = status.retweeted_status
        if hasattr(rstatus, 'quoted_status'):
            qstatus = rstatus.quoted_status
            qtweet  = tweet.TweetBody (
                tweet_id=qstatus['id'],
                author=qstatus['user']['screen_name'],
                content=qstatus['full_text'],
                embedded_tweet=None,
                embedded_url=None
            )
            retweet_text = strip_last_twitter_link(rstatus.full_text)
        else:
            qtweet = None
            retweet_text = rstatus.full_text

        retweet_text =  re.sub("\\s+", " ", retweet_text)

        retweet = tweet.TweetBody(
            tweet_id=rstatus.id,
            author=rstatus.user.screen_name,
            content=retweet_text,
            embedded_tweet=qtweet,
            embedded_url=None
        )
        tweet_text = ""
    elif hasattr(status, 'quoted_status'):
        qstatus = status.quoted_status
        retweet = tweet.TweetBody(
            tweet_id=qstatus['id'],
            author=qstatus['user']['screen_name'],
            content= re.sub("\\s+", " ", qstatus['full_text']),
            embedded_tweet=None,
            embedded_url=None
        )
        tweet_text = strip_last_twitter_link(status.full_text)
    else:
        retweet = None
        tweet_text = status.full_text

    # Get rid of raw-newlines in text
    tweet_text = re.sub("\\s+", " ", tweet_text)

    return tweet.TweetEnvelope(
        local_date=local_date,
        utc_date=status.created_at,
        tweet=tweet.TweetBody(
            tweet_id=status.id,
            author=status.user.screen_name,
            content=tweet_text,
            embedded_tweet=retweet,
            embedded_url=None
        )
    )

def strip_last_twitter_link(text):
    return re.sub(LastTwitterLink, "", text)

