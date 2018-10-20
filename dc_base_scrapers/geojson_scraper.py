import datetime
from geojson_rewind import rewind
import hashlib
import json
from collections import OrderedDict
from dc_base_scrapers.common import (
    BaseScraper,
    get_data_from_url,
    save,
    summarise,
    sync_db_to_github,
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
        return json.dumps(feature, sort_keys=True)

    def get_data(self):  # pragma: no cover
        data_str = get_data_from_url(self.url)
        data_str = rewind(data_str.decode(self.encoding))
        data = json.loads(data_str)
        return (data_str.encode(self.encoding), data)

    def scrape(self):

        # load json
        data_str, data = self.get_data()
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
                    value = feature['properties'][field]
                    if isinstance(value, str):
                        record[field] = value.strip()
                    else:
                        record[field] = value

            # save to db
            save(['pk'], record, self.table)

        if self.key is None:
            sync_db_to_github(self.council_id, self.table, 'pk')
        else:
            sync_db_to_github(self.council_id, self.table, self.key)

        # print summary
        summarise(self.table)

        self.store_history(data_str, self.council_id)


class RandomIdGeoJSONScraper(GeoJsonScraper):

    def get_data(self):

        """
        Some WFS servers produce output with id fields that seem to
        be randomly generated. Strip the ids out so that we can
        detect when the actual data has changed and not just the ids.
        """

        data_str = get_data_from_url(self.url)
        data_str = rewind(data_str.decode(self.encoding))

        data = json.loads(data_str, object_pairs_hook=OrderedDict)

        for i in range(0, len(data['features'])):
            data['features'][i]['id'] = i
        data_str = json.dumps(data).encode(self.encoding)

        return (data_str, data)

    def store_history(self, data_str, council_id):

        # slightly different for legacy compatibility

        data = json.loads(
            data_str.decode(self.encoding),
            object_pairs_hook=OrderedDict)

        for i in range(0, len(data['features'])):
            del data['features'][i]['id']

        super().store_history(
            json.dumps(data).encode(self.encoding),
            council_id
        )
