import json
from collections import OrderedDict
from dc_base_scrapers.common import (
    get_data_from_url,
    save,
    sync_file_to_github
)


def format_json(json_str):
    return json.dumps(
        json.loads(json_str, object_pairs_hook=OrderedDict),
        sort_keys=True, indent=4
    )


class DataPressScraper:

    def __init__(self, base_url, council_id, dataset, return_format, extra_fields, encoding):
        self.url = None
        self.council_id = council_id
        self.base_url = base_url
        self.dataset = dataset
        self.return_format = return_format
        self.extra_fields = extra_fields
        self.encoding = encoding

    def get_data(self):  # pragma: no cover
        data_str = get_data_from_url(self.url)
        data = json.loads(data_str.decode(self.encoding))
        return (data_str, data)

    def scrape(self):

        self.url = "%s%s" % (self.base_url, self.dataset)
        return_url = None

        # load json
        data_str, data = self.get_data()
        print(
            "found %i %s resources" %
            (len(data['resources']), self.dataset)
        )

        for key, resource in data['resources'].items():

            # assemble record
            record = {
                'format': resource['format'],
                'url': resource['url'],
                'dataset': self.dataset,
            }
            for field in self.extra_fields:
                record[field] = resource[field]

            # save to db
            save(['dataset', 'format'], record, 'resources')

            if resource['format'] == self.return_format:
                return_url = resource['url']

        sync_file_to_github(
            self.council_id,
            self.dataset,
            format_json(data_str.decode(self.encoding))
        )

        return return_url
