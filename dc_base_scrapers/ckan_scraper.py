import json
from dc_base_scrapers.common import get_data_from_url, save


class CkanScraper:

    def __init__(self, base_url, dataset, return_format, extra_fields, encoding):
        self.url = None
        self.base_url = base_url
        self.dataset = dataset
        self.return_format = return_format
        self.extra_fields = extra_fields
        self.encoding = encoding

    def scrape(self):

        self.url = "%s%s" % (self.base_url, self.dataset)
        return_url = None

        # load json
        data_str = get_data_from_url(self.url)
        data = json.loads(data_str.decode(self.encoding))
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

            if resource['format'] == self.return_format:
                return_url = resource['url']

        return return_url
