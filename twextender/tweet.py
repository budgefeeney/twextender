"""
This module contains a representation of a tweet, and code for reading tweets from
a file written out by the Java based twitter spider.
"""
from dateutil import parser as dateparser
import os
from pathlib import Path


MinAsSecs  = 60
HourAsSecs = 60 * MinAsSecs
QuarterHourAsSecs = HourAsSecs / 4

class UrlCard:
    """
    Tweets that reference URLs, are formatted so that in some cases these URLs are
    represented by "cards". This contains the card details, and -- if it was read --
    the card content
    """
    def __init__(self, url, card_url, title=None, content=None):
        self.url      = url
        self.card_url = card_url
        self.title    = title
        self.content  = content

    @classmethod
    def from_str_fields(cls, fields, start=0):
        """
        Given a list of fields (e.g. parsed out of a tab-delimited line) build
        a UriCard from that content. Optionally start mid-way through the parts
        using the start parameter

        Return a tuple of a UrlCard and the new start position
        """
        url      = fields[start + 0]
        card_url = fields[start + 1]

        present  = fields[start + 2].strip().upper() == 'D'
        if present:
            title   = fields[start + 3]
            content = fields[start + 4]
            next    = start + 5
        else:
            title   = None
            content = None
            next    = start + 3

        return UrlCard(url, card_url, title=title, content=content), next

    def to_str_fields(self):
        if self.title is None and self.content is None:
            return [self.url, self.card_url, "P"]
        else:
            return [self.url, self.card_url, "D", self.title, self.content]

    def __str__(self):
        if self.title is not None:
            return self.title + ": " + self.content + "(" + self.uri + ")"
        else:
            return self.url + "(detail at " + self.card_url + ")"


class TweetBody:
    """
    The bulk of the content of a tweet. All that's missing is the <code>TweetEnvelope</code>
    which will contain date information and the account in which this tweet appeared (some
    tweets may appear in different accounts if they're directly retweeted)
    """
    def __init__(self, tweet_id, author, content, embedded_url, embedded_tweet):
        """
        Build a tweet
        :param tweet_id: The ID of this tweet
        :param author: The screen-name of the person who wrote this tweet
        :param content: the content of the tweet
        :param embedded_url: the content of a URL associated with this tweet, this is a UrlCard
        object
        :param embedded_tweet: if this is a quote-tweet (i.e. a retweet with commentary) then this
        is the retweet which is being commented on in the <code>content</code>
        """
        self.tweet_id = int(tweet_id)
        self.author = author
        self.content = content
        self.embedded_url = embedded_url
        self.embedded_tweet = embedded_tweet

    def to_str_fields(self):
        core = [ self.author, str(self.tweet_id), self.content ]
        url_card = [ "none" ] if self.embedded_url is None \
            else   [ "some" ] + self.embedded_url.to_str_fields()
        embed_tweet = [ "none" ] if self.embedded_tweet is None \
            else      [ "some" ] + self.embedded_tweet.to_str_fields()

        return core + url_card + embed_tweet



    @classmethod
    def from_str_fields(cls, fields, start=0):
        """
        Given a list of fields (e.g. parsed out of a tab-delimited line) build a TweetBody
        object. Return a tuple of the  TweetBody and the next position to read from.
        """
        author = fields[start + 0]
        id     = int(fields[start + 1])
        msg    = fields[start + 2]

        url_card, next_start = \
            UrlCard.from_str_fields(fields, start + 4) \
            if fields[start + 3].lower() == "some" \
            else (None, start + 4)

        embed_tweet, next_next_start = \
            TweetBody.from_str_fields(fields, next_start + 1) \
            if fields[next_start].lower() == "some" \
            else (None, next_start + 1)

        return TweetBody(id, author, msg, url_card, embed_tweet), next_next_start



class TweetEnvelope:
    """
    The "envelope" for a tweet is the date and account information for a tweet. A tweet
    in this case is actually a chain of tweets containing tweets containing tweets
    """
    def __init__(self, utc_date, local_date, tweet):
        self.utc_date   = utc_date
        self.local_date = local_date
        self.tweet      = tweet


    @classmethod
    def from_str(cls, line):
        """
        Parse a tweet envelope (which includes its inner tweet) from the given fields,
        which have been parsed out of for example a tab-delimited text file. Optionally
        start at zero.

        Return a tuple with a TweetEnvelope and the next position to read from
        """
        fields = line.split('\t')

        # Re-order this to be a bit more amenable to the layout in this project
        local_date = dateparser.parse(fields[0])
        utc_date   = dateparser.parse(fields[1])
        # the third field is time-zone difference which we ignore
        tweet, _   = TweetBody.from_str_fields(fields, 3)

        return TweetEnvelope(utc_date=utc_date, local_date=local_date, tweet=tweet)

    def __str__(self):
        # Figure out the timezone string
        diff = self.utc_date - self.local_date
        rounded_diff = int(diff.total_seconds() / QuarterHourAsSecs) * QuarterHourAsSecs
        hours = int (rounded_diff / HourAsSecs)
        mins  = int ((rounded_diff % HourAsSecs) / MinAsSecs)

        tz_str = "%02d:%02d" % (hours, mins)

        # Then write it out.
        fields = [ self.local_date.isoformat(), self.utc_date.isoformat(), tz_str] + self.tweet.to_str_fields()
        return "\t".join(fields)



def tweet_files(dir):
    """
    Given a directory of tweet files broken down by category, so it's laid out in
    the form <parent>/<cat-dir>/<screen-name>.<count> return the full list of files.
    This is returned as a map from categories to a map of users to a list of user-files
    which is sorted
    """
    result = dict()
    sub_dirs = os.listdir(dir)
    for sub_dir_name in sub_dirs:
        sub_dir_name = dir + os.sep + sub_dir_name
        sub_dir = Path(sub_dir_name)
        if (not sub_dir.is_dir()) or sub_dir.name.startswith("."):
            continue

        sub_dir_contents = [sub_dir_name + os.sep + f for f in os.listdir(sub_dir_name)]
        user_files = [f for f in sub_dir_contents if is_visible_file(f)]
        if len(user_files) == 0:
            continue

        catgy_list = dict()
        result[sub_dir.name] = catgy_list

        user_files.sort(reverse=True)

        current_user_batch = [user_files.pop()]
        current_user_name  = screen_name_from_tweets_file(user_files[0])
        while len(user_files) > 0:
            f = user_files.pop()
            p = Path(f)
            fname = p.name

            if fname.startswith(current_user_name):
                current_user_batch.insert(0, f)
            else:
                catgy_list[current_user_name] = current_user_batch
                current_user_name = screen_name_from_tweets_file(f)
                current_user_batch = [f]

        catgy_list[current_user_name] = current_user_batch

    return result

def min_ids (catuserfiles):
    """
    Given a map of category -> user -> user-files (As returned by <code>tweet_Files</code>)
    return a map of category -> user -> tweet_id, with the oldest tweet-id for each user.
    """
    result = dict()
    for caty, usermap in catuserfiles.items():
        catmap = dict()
        result[caty] = catmap
        for user, userfiles in usermap.items():
            with open (userfiles.pop(), "r") as f:
                envelope = TweetEnvelope.from_str(f.readline())
                print(str(envelope))
                catmap[user] = envelope.tweet.tweet_id

    return result

def is_visible_file(f):
    p = Path(f)
    return p.is_file() and (not p.name.startswith("."))

def screen_name_from_tweets_file (filename):
    filename = Path(filename).name
    pos = filename.rfind(".")
    if pos < 0:
        return filename
    elif (filename[pos+1:]).isdigit():
        return filename[:pos]
    else:
        raise ValueError("Invalid file name : " + filename)


if __name__ == "__main__":
    lst = tweet_files("/Users/bryanfeeney/Desktop/SpiderUpTest/")
    mp  = min_ids(lst)

    print(str(mp))





