"""
A collection of classes and functions for managing a journal of twitter requests for
certain users.

This is to help synchronise between concurrent processes spidering the same group of
users, and also to help ensure that the application can easily restart and continue
where it left off should something go wrong.

The "journal" in this case is a directory of per-user journals: this is just to
ensure we don't spend too long reading a journal, and block other processes.
"""

from enum import Enum
from fcntl import flock, LOCK_EX, LOCK_NB, LOCK_UN
import time
import errno
from datetime import datetime
import dateutil.parser as dateparser
from random import random
from pathlib import Path
import os

JOURNAL_FILE_EXT = ".journal"

JOURNAL_ACCESS_TIMEOUT_SECS=3 * 60 # How long will it take to read and parse an entire journal
TRANSACTION_EXPIRY_TIMEOUT_SECS=5 * 60

class JournalResultType (Enum):
    NotFound = 1
    Found = 2
    InUse = 3
    BrokenJournal = 99

class JournalResponse:
    """
    The response to a query on the journal. Note that max_id and last_access
    may be undefined if the result_type is NotFound
    """
    def __init__(self, result_type, user, max_id, last_access, last_tweet_date_utc):
       self.result_type  = result_type
       self.user         = user
       self._max_id      = max_id
       self._last_access = last_access
       self._last_tweet_date_utc = last_tweet_date_utc

    @classmethod
    def not_found(cls, user_name):
        return JournalResponse(JournalResultType.NotFound, user_name, None, None, None)
    @classmethod
    def found(cls, j_entry):
        return JournalResponse(JournalResultType.Found, j_entry.user_name, j_entry.new_max_id, j_entry.date, j_entry.new_max_date)
    @classmethod
    def in_use(cls, j_entry):
        return JournalResponse(JournalResultType.InUse, j_entry.user_name, j_entry.old_max_id, j_entry.date, None)
    @classmethod
    def broken_journal(cls, user_name):
        return JournalResponse(JournalResultType.BrokenJournal, user_name, None, None, None)

    @property
    def max_id(self):
        if self.result_type is JournalResultType.NotFound:
            raise ValueError ("Cannot access max_id for user which was not found")
        elif self.result_type is JournalResultType.BrokenJournal:
            raise ValueError ("Cannot access max_id for user, journal is inaccessible")

        return self._max_id

    @property
    def last_access(self):
        if self.result_type is JournalResultType.NotFound:
            raise ValueError ("Cannot access last_access for user which was not found")
        elif self.result_type is JournalResultType.BrokenJournal:
            raise ValueError ("Cannot access last_Access for user, journal is inaccessible")

        return self._last_access

    @property
    def last_tweet_date_utc(self):
        if self.result_type is not JournalResultType.Found:
            raise ValueError ("Cannot access last_tweet_date_utc for if user who was not found")

        return self._last_tweet_date_utc

    def __str__(self):
        return self.result_type.name + ': [' + self.user + '] ' + str(self._max_id) + '@' + str(self._last_access)

class JournalEntryType(Enum):
    Started   = 0
    Abandoned = 1
    Finished  = 2

class JournalEntry:
    """
    An entry in a journal file.
    """
    def __init__(self, date, user_name, entry_type, old_max_id, new_max_id, new_max_date):
        self.date           = date
        self.user_name      = user_name
        self._lwr_user_name = user_name.lower()
        self.entry_type     = entry_type
        self.old_max_id     = old_max_id
        self.new_max_id     = new_max_id
        self.new_max_date   = new_max_date

    @classmethod
    def started_now(cls, user_name, from_max_id):
        return JournalEntry(datetime.utcnow(), user_name, JournalEntryType.Started, from_max_id, None, None)

    @classmethod
    def finished_now(cls, user_name, old_max_id, new_max_id, new_max_date):
        return JournalEntry(datetime.utcnow(), user_name, JournalEntryType.Finished, old_max_id, new_max_id, new_max_date)

    @classmethod
    def abandoned_now(cls, user_name, old_max_id):
        return JournalEntry(datetime.utcnow(), user_name, JournalEntryType.Abandoned, old_max_id, None, None)

    def is_for_user(self, user_name):
        """
        Case insensitive check to see if this journal-entry is for the given user.
        Returns true if it is, false otherwise
        """
        return user_name.lower() == self._lwr_user_name

    def is_completion_of(self, entry):
        """
        Every Twitter lookup has two parts: the initiation of the network traffic,
        and the completion of it. These have two parts, a Start and either an
        Abandoned or a Finished.

        This checks to see if the given entry completes (therefore is a Finished
        or Abandoned) the transaction started by this one (which must therefore be
        started), by seeing do the max_ids align.
        """
        if not self.entry_type is JournalEntryType.Started:
            return False

        if entry.entry_type is JournalEntryType.Started:
            return False

        return self.is_for_user(entry.user_name) \
            and (self.old_max_id is None or self.old_max_id == entry.old_max_id)

    def is_expired(self):
        """
        Checks the date of this entry against the current date: if the interval
        in minutes is bigger than TRANSACTION_EXPIRY_TIMEOUT_SECS then return True (expired)
        else return False (still active)
        """
        return (datetime.utcnow() - self.date).total_seconds() > TRANSACTION_EXPIRY_TIMEOUT_SECS

    @classmethod
    def from_str(cls, line):
        parts = line.strip().split('\t')
        entry_date = dateparser.parse(parts[0])
        user_name = parts[1]
        entry_type = JournalEntryType[parts[2]]

        old_id, new_id, last_access_date = None, None, None
        if len(parts) > 3:
            old_id = None if parts[3] == str(None) else int(parts[3])
        if len(parts) > 4:
            new_id = None if parts[4] == str(None) else int(parts[4])
        if len(parts) > 5:
            last_access_date = None if parts[5] == str(None) else dateparser.parse(parts[5])

        result = JournalEntry(
            entry_date,
            user_name,
            entry_type,
            old_max_id=old_id,
            new_max_id=new_id,
            new_max_date=last_access_date
        )

        return result

    def __str__(self):
        return       self.date.isoformat() \
            + '\t' + self.user_name \
            + '\t' + self.entry_type.name \
            + '\t' + (str(None) if self.old_max_id   is None else str(self.old_max_id)) \
            + '\t' + (str(None) if self.new_max_id   is None else str(self.new_max_id)) \
            + '\t' + (str(None) if self.new_max_date is None else self.new_max_date.isoformat())





class Journal:
    """
    A journal records progress, and allows the app to restart if it crashes, and to
    synchronise if necessary with other instances of the app which are already
    running.

    Before processing a user asks the journal for the maxID of a user. The responses
    are
    a user: tweets earlier than this ID are fetched. If no record is found, it's up to
    the app to find the ID elsewhere (e.g. by opening up a user's tweets file). The
    app will then download a batch of tweets from Twitter, and, once these have been
    successfully written out, will return them to the file.

    The journal gradually builds up two entries of the form
    <job-date><user>STARTED<previous-max-id>
    <job-date><user>FINISHED<previous-max-id><new-max-id><new-max-date>

    The minimum date is taken from the tweets themselves, and is the date associated
    with the tweet which has the "new" maxID.

    If a journal looks up a result for a user, finds it has been started, but not
    finished within a time limit, it adds an FAILURE record

    <job-date><user>FAILURE<previous-max-id>
    """

    def __init__(self, journal_dir):
        """
        Creates a new journal object
        :param journal_dir: the directory where the journals should go.
        """
        self._journal_dir = journal_dir
        path = Path(journal_dir)
        if not path.exists():
            path.mkdir(parents=False)

    def abandon(self, user_name, old_max_id):
        """
        Record that we had to abandon the last twitter read attempt
        """
        entry = JournalEntry.abandoned_now(user_name, old_max_id)
        with open(self._journal_for_user(user_name), "a") as f:
            try_lock(f, JOURNAL_ACCESS_TIMEOUT_SECS)
            try:
                f.write(str(entry) + '\n')
            finally:
                unlock(f)

    def finish(self, user_name, old_max_id, new_max_id, new_max_date):
        """
        Record that we've finished processing a user, having started reading tweets
        earlier than old_max_id, and having read a batch whose minimum is new_max_id
        """
        entry = JournalEntry.finished_now(user_name, old_max_id, new_max_id, new_max_date)
        with open(self._journal_for_user(user_name), "a") as f:
            try_lock(f, JOURNAL_ACCESS_TIMEOUT_SECS)
            try:
                f.write(str(entry) + '\n')
            finally:
                unlock(f)

    def try_start(self, user_name, from_max_id=None):
        """
        Called when an application tries to start processing a given user.
        Return value is NotFound, if this is the first time spidering that user,
        FOUND if we're resuming with that user, or InUse if the user is currently
        being processed by a different process.

        :param user_name: the user whose progress to record
        :param from_max_id: if this is _not_ None, then we don't bother checking
        the journal, we just directly write a started entry, and return an InUse
        response
        :return: False if a record for this user already exists in the file,
        True otherwise
        """
        with open(self._journal_for_user(user_name), "r+") as f:
            try:
                try_lock(f, JOURNAL_ACCESS_TIMEOUT_SECS)

                # If we've been given a max_id, don't bother checking the journal
                if from_max_id is not None:
                    f.seek(0, 2) # Go to the end (zero-bytes before SEEK_END=2)
                    entry = JournalEntry.started_now(user_name, from_max_id)
                    f.write(str(entry) + '\n')
                    return JournalResponse.in_use(entry)
                else:

                    # Find all the journal entries for the given user. Condense
                    # start-finish pairs to just a single finished record (do the
                    # same for abandoned).
                    user_entries = []
                    for line in f:
                        line = line.strip()
                        if len(line) == 0:
                            continue

                        entry = JournalEntry.from_str(line)

                        if not entry.is_for_user(user_name):
                            raise ValueError ("Invalid journal file, wrong user found")

                        if len(user_entries) == 0:
                            user_entries.append(entry)
                        else:
                            if entry.is_completion_of(user_entries[-1]):
                                user_entries.pop()
                            user_entries.append(entry)

                    # Go back to last successfully completed journal entry
                    # Immediately write a record to the journal once we've found it
                    while len(user_entries) > 0:
                        l = user_entries.pop()
                        if l.entry_type is JournalEntryType.Started:
                            if not l.is_expired():
                                return JournalResponse.in_use(l)
                        elif l.entry_type is JournalEntryType.Finished:
                            resp     = JournalResponse.found(l)
                            newEntry = JournalEntry.started_now(user_name, resp.max_id)
                            f.write(str(newEntry) + '\n')
                            return resp

                    newEntry = JournalEntry.started_now(user_name, None)
                    f.write(str(newEntry) + '\n')
                    return JournalResponse.not_found(user_name)
            finally:
                unlock(f)


    def journalled_users(self):
        """
        Return a list of users that are journalled
        """
        journal_files = os.listdir(self._journal_dir)
        maybe_users = [self._user_for_journal(self._journal_dir + os.sep + f) for f in journal_files]

        return [u for u in maybe_users if u is not None]


    def _journal_for_user (self, user_name):
        """
        Return the journal file for the given user. Ensure it exists.
        :param user_name: the "screen name" of a twitter user.
        :return: a path (as a string) to a file.
        """
        journal_file = self._journal_dir + "/" + user_name.lower() + JOURNAL_FILE_EXT
        Path(journal_file).touch(exist_ok=True)
        return journal_file

    def _user_for_journal(self, user_journal_path):
        """
        Return the screen name associated with a given journal file. If the file in
        question does not match the expected journal format, return None instead.
        """
        path = Path(user_journal_path)
        if not path.is_file():
            return None
        name = path.name

        if name.startswith("."):
            return None
        if not name.endswith(JOURNAL_FILE_EXT):
            return None

        return name[0:-len(JOURNAL_FILE_EXT)]



def try_lock(fd, timeout_secs):
    """
    Tries to lock the given file. Waits a total of timeout_secs to do so.
    If it can't get a lock in that time, raises a BlockingIOError
    """
    while True:
        try:
            flock(fd, LOCK_EX | LOCK_NB)
            break

        except IOError as e:
            # raise on unrelated IOErrors
            if e.errno != errno.EAGAIN or timeout_secs <= 0:
                raise
            else:
                timeout_secs -= 0.1
                time.sleep(0.05 + random() * 0.05)

def unlock(fd):
    """
    Unlocks the given file, locked with try_lock()
    """
    flock(fd, LOCK_UN)


if __name__ == "__main__":
    JOURNAL_FILE = "/tmp/journalfile"
    journal_path = Path(JOURNAL_FILE)
    if journal_path.exists():
        journal_path.unlink() # Get rid of the old journal before testing.

    journal_1 = Journal(JOURNAL_FILE)
    journal_2 = Journal(JOURNAL_FILE)

    sample_ids = [7594930202 - 6, 7594930202 - 5, 7594930202 - 4, 7594930202 - 3, 7594930202 - 2, 7594930202 - 1, 7594930202 - 0]

    resps = []

    resps.append (journal_1.try_start("bob")) # 0
    resps.append (journal_1.try_start("bob", sample_ids.pop())) # 1
    resps.append (journal_2.try_start("bob")) # 2
    resps.append (journal_2.try_start("alice")) # 3
    resps.append (journal_2.try_start("alice", sample_ids.pop())) # 4
    resps.append (journal_1.try_start("Bob")) # 5
    resps.append (journal_1.try_start("eve")) # 6
    journal_1.finish ("bob", old_max_id=resps[1].max_id, new_max_id=sample_ids.pop(), new_max_date=datetime.utcnow())
    journal_2.finish("alice", old_max_id=resps[4].max_id, new_max_id=sample_ids.pop(), new_max_date=datetime.utcnow()) # 7
    journal_1.abandon ("eve", resps[5].max_id)
    resps.append (journal_2.try_start("Alice"))
    journal_2.finish("Alice", old_max_id=resps[-1].max_id, new_max_id=sample_ids.pop(), new_max_date=datetime.utcnow())
    resps.append (journal_2.try_start("bob"))
    journal_2.finish("Bob", old_max_id=resps[-1].max_id, new_max_id=sample_ids.pop(), new_max_date=datetime.utcnow())

    for r in resps:
        print (str(r))