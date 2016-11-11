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


def save(unique_keys, data, table_name):
    scraperwiki.sqlite.save(
        unique_keys=unique_keys,
        data=data,
        table_name=table_name)
    scraperwiki.sqlite.commit_transactions()


@retry(HTTPError, tries=2, delay=30)
def get_data_from_url(url):
    with urllib.request.urlopen(url) as response:
        data = response.read()
        return data


class BaseScraper(metaclass=abc.ABCMeta):

    store_raw_data = False

    def __init__(self):
        if not hasattr(self, 'table'):
            raise NotImplementedError('Subclasses must define table')

    @abc.abstractmethod
    def scrape(self):
        pass

    def store_history(self, data, council_id):
        """
        store a hash of the content with a timestamp so we can work out
        when the content has changed from inside the scraper
        """
        hash_record = {
            'council_id': council_id,
            'timestamp': datetime.datetime.now(),
            'table': self.table,
            'content_hash': hashlib.sha1(data).hexdigest(),
        }
        if self.store_raw_data:
            hash_record['raw_data'] = data

        save(['timestamp'], hash_record, 'history')
