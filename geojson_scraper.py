import json
import os
import urllib.request
from common import store_history, truncate, summarise

# hack to override sqlite database filename
# see: https://help.morph.io/t/using-python-3-with-morph-scraperwiki-fork/148
os.environ['SCRAPERWIKI_DATABASE_NAME'] = 'sqlite:///data.sqlite'
import scraperwiki


def scrape(url, council_id, encoding, table):

    # clear any existing data
    truncate(table)

    with urllib.request.urlopen(url) as response:

        # load json
        data_str = response.read()
        data = json.loads(data_str.decode(encoding))
        print("found %i %s" % (len(data['features']), table))

        for feature in data['features']:

            # assemble record
            record = {
                'pk': feature['id'],
                'council_id': council_id,
                'geometry': json.dumps(feature),
            }
            for field in feature['properties']:
                if field != 'bbox':
                    record[field] = feature['properties'][field]

            # save to db
            scraperwiki.sqlite.save(
                unique_keys=['pk'],
                data=record,
                table_name=table)
            scraperwiki.sqlite.commit_transactions()

        # print summary
        summarise(table)

        store_history(data_str, table)
