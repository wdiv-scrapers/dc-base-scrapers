import datetime
import hashlib
import os

# hack to override sqlite database filename
# see: https://help.morph.io/t/using-python-3-with-morph-scraperwiki-fork/148
os.environ['SCRAPERWIKI_DATABASE_NAME'] = 'sqlite:///data.sqlite'
import scraperwiki


def store_history(data, table):
    """
    store a hash of the content with a timestamp so we can work out
    when the content has changed from inside the scraper
    """
    hash_record = {
        'timestamp': datetime.datetime.now(),
        'table': table,
        'content_hash': hashlib.sha1(data).hexdigest()
    }
    scraperwiki.sqlite.save(
        unique_keys=['timestamp'],
        data=hash_record,
        table_name='history')
    scraperwiki.sqlite.commit_transactions()


def truncate(table):
    scraperwiki.sqlite.execute("DROP TABLE IF EXISTS %s;" % table)
    scraperwiki.sqlite.commit_transactions()


def summarise(table):
    count = scraperwiki.sqlite.execute(
        "SELECT COUNT(*) AS count FROM %s;" % table)
    print("%i %s in database" % (count['data'][0].count, table))
