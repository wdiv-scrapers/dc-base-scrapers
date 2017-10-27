import abc
import base64
import datetime
import hashlib
import json
import os
import requests
import scraperwiki
import urllib.request
from collections import OrderedDict
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
        repo=os.environ['MORPH_GITHUB_REPO'],
        branch='master',
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


def sync_file_to_github(council_id, file_name, content):
    try:
        creds = get_github_credentials()
        g = GitHubClient(creds)
        path = "%s/%s" % (council_id, file_name)

        # if we haven't specified an extension, assume .json
        if '.' not in path:
            path = "%s.json" % (path)

        g.put_file(content, path)
    except KeyError:
        # if no credentials are defined in env vars
        # just ignore this step
        pass


def sync_db_to_github(council_id, table_name, key):
    content = dump_table_to_json(table_name, key)
    sync_file_to_github(council_id, table_name, content)


@retry(HTTPError, tries=2, delay=30)
def get_data_from_url(url):
    with urllib.request.urlopen(url, timeout=300) as response:
        data = response.read()
        return data


def force_bytes(data, encoding='utf-8'):
    if isinstance(data, (bytes, bytearray)):
        return data
    return bytes(data, encoding)


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


class GitHubCredentials:

    def __init__(self, repo, branch, name, email, api_key):
        self.repo = repo
        self.branch = branch
        self.name = name
        self.email = email
        self.api_key = api_key


class GitHubClient:

    def __init__(self, credentials):
        if not isinstance(credentials, GitHubCredentials):
            raise TypeError('expected GitHubCredentials object')
        self.credentials = credentials

    def get_payload(self, content, parent_sha=None):
        # assemble a payload we can use to make a request
        # to the /contents endpoint in the GitHub API
        # https://developer.github.com/v3/repos/contents/#create-a-file
        payload = {
            'message': 'Update data %s' % (str(datetime.datetime.now())),
            'content': base64.b64encode(force_bytes(content)).decode('utf-8'),
            'branch': self.credentials.branch,
            "committer": {
                "name": self.credentials.name,
                "email": self.credentials.email
            },
        }
        # if we're updating, we need to set the 'sha' key
        # https://developer.github.com/v3/repos/contents/#update-a-file
        if parent_sha:
            payload['sha'] = parent_sha
        return json.dumps(payload)

    def get_file(self, filename):
        url = 'https://raw.githubusercontent.com/%s/%s/%s' % (
            self.credentials.repo, self.credentials.branch, filename)
        r = requests.get(url)
        r.raise_for_status()
        return r.content

    def get_blob_sha(self, data):
        # work out the git SHA of a blob
        # (this is not the same as the commit SHA)
        # https://stackoverflow.com/questions/552659/how-to-assign-a-git-sha1s-to-a-file-without-git/552725#552725
        s = sha1()
        s.update(("blob %u\0" % len(force_bytes(data))).encode('utf-8'))
        s.update(force_bytes(data))
        return s.hexdigest()

    def put_file(self, content, filename):
        try:
            repo_content = self.get_file(filename)
            # check if we need to do a commit because the /contents
            # endpoint will allow us to make an empty commit
            if force_bytes(content) == repo_content:
                payload = None
            else:
                payload = self.get_payload(content, self.get_blob_sha(repo_content))
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                payload = self.get_payload(content)
            else:
                raise

        if payload:
            url = 'https://api.github.com/repos/%s/contents/%s' % (
                self.credentials.repo, filename)
            r = requests.put(url,
                data=payload,
                headers={'Authorization': 'token %s' % (self.credentials.api_key)}
            )
            if r.status_code not in [200, 201]:
                print(r.json())
            r.raise_for_status()
