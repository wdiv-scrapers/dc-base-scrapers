import json
from collections import OrderedDict
from dc_base_scrapers.common import (
    format_json,
    get_data_from_url,
    save,
    sync_file_to_github
)


class CkanScraper:

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
            (len(data['result']['resources']), self.dataset)
        )

        for resource in data['result']['resources']:

            # assemble record
            record = {
                'format': resource['format'],
                'revision_id': resource['revision_id'],
                'created': resource['created'],
                'url': resource['url'],
                'dataset': self.dataset,
            }
            for field in self.extra_fields:
                record[field] = resource[field]

            # save to db
            save(['dataset', 'revision_id', 'format'], record, 'resources')

            if resource['format'].lower() == self.return_format.lower():
                return_url = resource['url']

        sync_file_to_github(
            self.council_id,
            self.dataset,
            format_json(data_str.decode(self.encoding))
        )

        return return_url
