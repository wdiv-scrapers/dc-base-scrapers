import abc
import datetime
import json
import os
import scraperwiki
import urllib.request
from collections import OrderedDict
from commitment import GitHubCredentials, GitHubClient
from hashlib import sha1
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


def get_github_credentials():
    return GitHubCredentials(
        repo=os.environ['MORPH_GITHUB_POLLING_REPO'],
        name=os.environ['MORPH_GITHUB_USERNAME'],
        email=os.environ['MORPH_GITHUB_EMAIL'],
        api_key=os.environ['MORPH_GITHUB_API_KEY']
    )


def dump_table_to_json(table_name, key):
    if isinstance(key, list):
        order_clause = ", ".join(key)
    else:
        order_clause = key
    records = scraperwiki.sqlite.select(
        " * FROM %s ORDER BY %s;" % (table_name, order_clause))
    return json.dumps(
        [OrderedDict(sorted(rec.items())) for rec in records],
        sort_keys=True, indent=4)


def format_json(json_str, exclude_keys=None):
    data = json.loads(json_str, object_pairs_hook=OrderedDict)
    if isinstance(data, dict) and exclude_keys:
        for key in exclude_keys:
            data.pop(key, None)
    return json.dumps(data, sort_keys=True, indent=4 )


def sync_file_to_github(council_id, file_name, content):
    try:
        creds = get_github_credentials()
        g = GitHubClient(creds)
        path = "%s/%s" % (council_id, file_name)

        # if we haven't specified an extension, assume .json
        if '.' not in path:
            path = "%s.json" % (path)

        g.push_file(content, path, 'Update %s at %s' %\
            (path, str(datetime.datetime.now())))
    except KeyError:
        # if no credentials are defined in env vars
        # just ignore this step
        pass


def sync_db_to_github(council_id, table_name, key):
    content = dump_table_to_json(table_name, key)
    sync_file_to_github(council_id, table_name, content)


@retry(HTTPError, tries=2, delay=30)
def get_data_from_url(url):  # pragma: no cover
    with urllib.request.urlopen(url, timeout=300) as response:
        if response.code == 202:
            raise HTTPError(
                url,
                response.code,
                response.msg,
                response.headers,
                response.fp
            )
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
            'content_hash': sha1(data).hexdigest(),
        }
        if self.store_raw_data:
            hash_record['raw_data'] = data

        save(['timestamp'], hash_record, 'history')
