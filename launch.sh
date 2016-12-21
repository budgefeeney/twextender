#!/bin/bash

SRC_DIR=`dirname $0`
cd $SRC_DIR

PYTHON=`which python3.4`

JOURNAL_DIR=/Users/bryanfeeney/opt-hillary/twextender.journal
TWEETS_DIR=/Users/bryanfeeney/opt-hillary/twitter-tools-spider/src/test/resources/spider/_historic
TARGET_DATE="2016-07-01"

$PYTHON main.py -d $TARGET_DATE -p $JOURNAL_DIR -t $TWEETS_DIR
