import json
from arcgis2geojson import arcgis2geojson
from dc_base_scrapers.common import (
    BaseScraper,
    get_data_from_url,
    save,
    summarise,
    truncate,
)


class ArcGisScraper(BaseScraper):

    def __init__(self, url, council_id, encoding, table, store_raw_data=False):
        self.url = url
        self.council_id = council_id
        self.encoding = encoding
        self.table = table
        self.store_raw_data = store_raw_data
        super().__init__()

    def make_geometry(self, feature):
        return json.dumps(arcgis2geojson(feature))

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

            # assemble record
            record = {
                'council_id': self.council_id,
                'geometry': self.make_geometry(feature),
            }
            for field in fields:
                record[field['name']] = feature['attributes'][field['name']]

            # save to db
            save(['OBJECTID'], record, self.table)

        # print summary
        summarise(self.table)

        self.store_history(data_str)
