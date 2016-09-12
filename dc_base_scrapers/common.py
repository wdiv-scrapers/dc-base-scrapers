import abc
import datetime
import hashlib
import scraperwiki
import urllib.request
from retry import retry
from urllib.error import HTTPError


def truncate(table):
    scraperwiki.sqlite.execute("DROP TABLE IF EXISTS %s;" % table)
    scraperwiki.sqlite.commit_transactions()


def summarise(table):
    count = scraperwiki.sqlite.execute(
        "SELECT COUNT(*) AS count FROM %s;" % table)
    print("%i %s in database" % (count['data'][0].count, table))


@retry(HTTPError, tries=2, delay=30)
def get_data_from_url(url):
    with urllib.request.urlopen(url) as response:
        data = response.read()
        return data


class BaseScraper(metaclass=abc.ABCMeta):

    def __init__(self):
        if not hasattr(self, 'table'):
            raise NotImplementedError('Subclasses must define table')

    @abc.abstractmethod
    def scrape(self):
        pass

    def store_history(self, data):
        """
        store a hash of the content with a timestamp so we can work out
        when the content has changed from inside the scraper
        """
        hash_record = {
            'timestamp': datetime.datetime.now(),
            'table': self.table,
            'content_hash': hashlib.sha1(data).hexdigest(),
            'raw_data': data,
        }
        scraperwiki.sqlite.save(
            unique_keys=['timestamp'],
            data=hash_record,
            table_name='history')
        scraperwiki.sqlite.commit_transactions()
