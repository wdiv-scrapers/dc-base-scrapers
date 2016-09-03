import json
import os
import urllib.request
from retry import retry
from urllib.error import HTTPError

# hack to override sqlite database filename
# see: https://help.morph.io/t/using-python-3-with-morph-scraperwiki-fork/148
os.environ['SCRAPERWIKI_DATABASE_NAME'] = 'sqlite:///data.sqlite'
import scraperwiki


@retry(HTTPError, tries=2, delay=30)
def scrape_resources(base_url, dataset, return_format, extra_fields, encoding):

    resource_url = "%s%s" % (base_url, dataset)
    return_url = None

    with urllib.request.urlopen(resource_url) as response:

        # load json
        data_str = response.read()
        data = json.loads(data_str.decode(encoding))
        print(
            "found %i %s resources" %
            (len(data['result']['resources']), dataset)
        )

        for resource in data['result']['resources']:

            # assemble record
            record = {
                'format': resource['format'],
                'revision_id': resource['revision_id'],
                'created': resource['created'],
                'url': resource['url'],
                'dataset': dataset,
            }
            for field in extra_fields:
                record[field] = resource[field]

            # save to db
            scraperwiki.sqlite.save(
                unique_keys=['dataset', 'revision_id', 'format'],
                data=record,
                table_name='resources')
            scraperwiki.sqlite.commit_transactions()

            if resource['format'] == return_format:
                return_url = resource['url']

    return return_url
