"""
This module contains a representation of a tweet, and code for reading tweets from
a file written out by the Java based twitter spider.
"""
from dateutil import parser as dateparser

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

        present  = fields[start + 2].strip().upper() == 'P'
        if present:
            title = fields[start + 3]
            body  = fields[start + 4]
            next  = start + 5
        else:
            title = None
            body  = None
            next  = start + 3

        return UrlCard(url, card_url, title=title, content=body), next

    def __str__(self):
        if self.title is not None:
            return self.title + ": " + self.body + "(" + self.uri + ")"
        else:
            return self.uri + "(detail at " + self.card_uri + ")"


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
        self.tweet_id = tweet_id
        self.author = author
        self.content = content
        self.embedded_url = embedded_url
        self.embedded_tweet = embedded_tweet


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
            else None, start + 4

        embed_tweet, next_next_start = \
            TweetBody.from_str_fields(fields, next_start + 1) \
            if fields[next_start].lower() == "some" \
            else None, next_start + 1

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
        tweet, _   = TweetBody.from_str_fields(fields, 2)

        return TweetEnvelope(utc_date=utc_date, local_date=local_date, tweet=tweet)




