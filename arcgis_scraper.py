import json
import os
import urllib.request
from arcgis2geojson import arcgis2geojson
from common import store_history, truncate, summarise

# hack to override sqlite database filename
# see: https://help.morph.io/t/using-python-3-with-morph-scraperwiki-fork/148
os.environ['SCRAPERWIKI_DATABASE_NAME'] = 'sqlite:///data.sqlite'
import scraperwiki


def scrape(url, council_id, encoding, table):

    with urllib.request.urlopen(url) as response:

        # clear any existing data
        truncate(table)

        # load json
        data_str = response.read()
        data = json.loads(data_str.decode(encoding))
        print("found %i %s" % (len(data['features']), table))

        # grab field names
        fields = data['fields']

        for feature in data['features']:

            # convert arcgis geometry to geojson
            geometry = arcgis2geojson(feature)

            # assemble record
            record = {
                'council_id': council_id,
                'geometry': json.dumps(geometry),
            }
            for field in fields:
                record[field['name']] = feature['attributes'][field['name']]

            # save to db
            scraperwiki.sqlite.save(
                unique_keys=['OBJECTID'],
                data=record,
                table_name=table)
            scraperwiki.sqlite.commit_transactions()

        # print summary
        summarise(table)

        store_history(data_str, table)
