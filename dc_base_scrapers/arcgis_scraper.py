import datetime
import hashlib
import json
from arcgis2geojson import arcgis2geojson
from collections import OrderedDict
from dc_base_scrapers.common import (
    BaseScraper,
    get_data_from_url,
    save,
    summarise,
    truncate,
)


class ArcGisScraper(BaseScraper):

    def __init__(self, url, council_id, encoding, table, key='OBJECTID', store_raw_data=False):
        self.url = url
        self.council_id = council_id
        self.encoding = encoding
        self.table = table
        self.key = key
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
                value = feature['attributes'][field['name']]
                if isinstance(value, str):
                    record[field['name']] = value.strip()
                else:
                    record[field['name']] = value

            # save to db
            save([self.key], record, self.table)

        # print summary
        summarise(self.table)

        self.store_history(data_str, self.council_id)


class UnsortedArcGisScraper(ArcGisScraper):

    def store_history(self, data_str, council_id):

        """
        Older versions of ArcGIS do not support orderByFields
        Sort the data before we hash the json so that we can
        detect when the actual data has changed and not just the order.
        """
        data = json.loads(
            data_str.decode(self.encoding),
            object_pairs_hook=OrderedDict)

        data = sorted(data['features'], key=lambda k: k['attributes'][self.key])
        data_str = json.dumps(data).encode(self.encoding)

        hash_record = {
            'council_id': council_id,
            'timestamp': datetime.datetime.now(),
            'table': self.table,
            'content_hash': hashlib.sha1(data_str).hexdigest(),
        }
        if self.store_raw_data:
            hash_record['raw_data'] = data_str

        save(['timestamp'], hash_record, 'history')


class RandomIdArcGisScraper(ArcGisScraper):

    def store_history(self, data_str, council_id):

        """
        Sometimes we find an ArcGIS server where the ID field is for some
        reason unstable. Strip the ID out before we hash the json so that
        we can detect when the actual data has changed and not just the ids.
        """
        data = json.loads(
            data_str.decode(self.encoding),
            object_pairs_hook=OrderedDict)

        for i in range(0, len(data['features'])):
            del data['features'][i]['attributes'][self.key]
        data_str = json.dumps(data).encode(self.encoding)

        hash_record = {
            'council_id': council_id,
            'timestamp': datetime.datetime.now(),
            'table': self.table,
            'content_hash': hashlib.sha1(data_str).hexdigest(),
        }
        if self.store_raw_data:
            hash_record['raw_data'] = data_str

        save(['timestamp'], hash_record, 'history')
