import json
from dc_base_scrapers.common import (
    BaseScraper,
    get_data_from_url,
    save,
    summarise,
    truncate,
)


class GeoJsonScraper(BaseScraper):

    def __init__(self, url, council_id, encoding, table, key=None, store_raw_data=False):
        self.url = url
        self.council_id = council_id
        self.encoding = encoding
        self.table = table
        self.key = key
        self.store_raw_data = store_raw_data
        super().__init__()

    def make_geometry(self, feature):
        return json.dumps(feature)

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
                'geometry': self.make_geometry(feature),
            }
            if self.key is None:
                record['pk'] = feature['id']
            else:
                record['pk'] = feature['properties'][self.key]

            for field in feature['properties']:
                if field != 'bbox':
                    record[field] = feature['properties'][field]

            # save to db
            save(['pk'], record, self.table)

        # print summary
        summarise(self.table)

        self.store_history(data_str)
