
"""
This app exists to download files from existing users.

It can do two things:
 1. If called with the "-c" option and a directory to downloaded tweets,
    it will create a journal which for every user will indicate the
    oldest tweet downloaded.
 2. If called with the "-p" option and path to a journal directory, it
    will resume downloading tweets up to a limit given by the "-d" flag,
    which is a yyyy-mm-dd formatted date.
"""
from optparse import OptionParser
import sys
from pathlib import Path
from twextender import journal
from twextender import tweet
from dateutil import parser as dateparser

def sanity_check(options, parser):
    """
    Prints error messages, usage, and then quits if the options given aren't sane
    """
    error = None
    if options.tweets_dir is None:
        error = "You must supply a path to a tweets directory"
    if options.output_journal is not None:
        out_path = Path(options.output_journal)
        if out_path.exists():
            error = "A file at the given journal path already exists. Refusing to overwrite";

        if options.input_journal is not None:
            error = "Cannot and create and process a journal at the same time, separate invocations must be used for each action"

        if options.target_date is not None:
            error = "Target date is not to be used when creating a journal"
    elif options.input_journal is not None:
        if options.target_date is None:
            error = "A target date must be specified when processing a journal"
        try:
            options.target_date = dateparser.parse(options.target_date)
        except (ValueError, OverflowError) as e:
            error = "Invalid date '" + options.target_date + "': " + str(e)

    if error is not None:
        sys.stderr.write(error + "\n")
        sys.stderr.flush()
        sys.stdout.flush()
        parser.print_help(sys.stderr)
        exit(-1)



if __name__ == "__main__":
    usage = "Usage: %prog [options]"
    parser = OptionParser()
    parser.add_option("-c", "--create-journal", dest="output_journal",
                      help="Create a journal saved at the given OUT path", metavar="OUT")
    parser.add_option("-p", "--process-journal", dest="input_journal",
                      help="Open the given INPUT journal and start downloading tweets", metavar="INPUT")
    parser.add_option("-d", "--target-date", dest="target_date", metavar="DATE",
                      help="When downloading tweets, stop once this threshold has been passed (going back)")
    parser.add_option("-t", "--tweets-dir", dest="tweets_dir", metavar="TDIR",
                      help="The directory where tweets are read from, or written to")

    (options, args) = parser.parse_args()
    sanity_check(options, parser)


    if options.output_journal is not None:


        lst = tweet.tweet_files(options.tweets_dir)
        ids_and_dates = tweet.min_ids_and_dates(lst)

        # The same user may have appeared many times due to appearancs in different categories
        user_ids = dict()
        for cat_map in ids_and_dates.values():
            for  user, (tweet_id, tweet_date) in cat_map.items():
                if user in user_ids:
                    if tweet_id < user_ids[user][0]:
                        user_ids[user] = (tweet_id, tweet_date)
                else:
                    user_ids[user] = (tweet_id, tweet_date)

        # With clean map, start creating a journal
        jrnl = journal.Journal(options.output_journal)
        for user, (tweet_id, tweet_date) in user_ids.items():
            jrnl.finish(user_name=user, old_max_id=-1, new_max_id=tweet_id, new_max_date=tweet_date)

        print (str(len(user_ids)) + " user-records written to journal at " + options.output_journal)
    elif options.input_journal is not None:
        if options.target_date is None:
            raise ValueError ("Need to specify a target date when processing a journal")
        pass
    else:
        sys.stderr.write ("Need to specify either a --create-journal or a --process journal action")
        parser.print_usage()
        exit()





