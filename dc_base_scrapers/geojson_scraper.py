import json
import scraperwiki
from dc_base_scrapers.common import (
    BaseScraper, truncate, summarise, get_data_from_url)


class GeoJsonScraper(BaseScraper):

    def __init__(self, url, council_id, encoding, table, key=None, store_raw_data=False):
        self.url = url
        self.council_id = council_id
        self.encoding = encoding
        self.table = table
        self.key = key
        self.store_raw_data = store_raw_data
        super().__init__()

    def scrape(self):

        # load json
        data_str = get_data_from_url(self.url)
        data = json.loads(data_str.decode(self.encoding))
        print("found %i %s" % (len(data['features']), self.table))

        # clear any existing data
        truncate(self.table)

        for feature in data['features']:

            # assemble record
            record = {
                'council_id': self.council_id,
                'geometry': json.dumps(feature),
            }
            if self.key is None:
                record['pk'] = feature['id']
            else:
                record['pk'] = feature['properties'][self.key]

            for field in feature['properties']:
                if field != 'bbox':
                    record[field] = feature['properties'][field]

            # save to db
            scraperwiki.sqlite.save(
                unique_keys=['pk'],
                data=record,
                table_name=self.table)
            scraperwiki.sqlite.commit_transactions()

        # print summary
        summarise(self.table)

        self.store_history(data_str)
