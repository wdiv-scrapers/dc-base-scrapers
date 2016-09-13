import json
import scraperwiki
from arcgis2geojson import arcgis2geojson
from dc_base_scrapers.common import (
    BaseScraper, truncate, summarise, get_data_from_url)


class ArcGisScraper(BaseScraper):

    def __init__(self, url, council_id, encoding, table, store_raw_data=False):
        self.url = url
        self.council_id = council_id
        self.encoding = encoding
        self.table = table
        self.store_raw_data = store_raw_data
        super().__init__()

    def scrape(self):

        # load json
        data_str = get_data_from_url(self.url)
        data = json.loads(data_str.decode(self.encoding))
        print("found %i %s" % (len(data['features']), self.table))

        # clear any existing data
        truncate(self.table)

        # grab field names
        fields = data['fields']

        for feature in data['features']:

            # convert arcgis geometry to geojson
            geometry = arcgis2geojson(feature)

            # assemble record
            record = {
                'council_id': self.council_id,
                'geometry': json.dumps(geometry),
            }
            for field in fields:
                record[field['name']] = feature['attributes'][field['name']]

            # save to db
            scraperwiki.sqlite.save(
                unique_keys=['OBJECTID'],
                data=record,
                table_name=self.table)
            scraperwiki.sqlite.commit_transactions()

        # print summary
        summarise(self.table)

        self.store_history(data_str)
