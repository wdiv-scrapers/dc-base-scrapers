from dc_base_scrapers.common import (
    BaseScraper,
    format_json,
    get_data_from_url,
    sync_file_to_github
)


class HashOnlyScraper(BaseScraper):

    def __init__(self, url, council_id, table, extension=None):
        self.url = url
        self.council_id = council_id
        self.table = table
        self.extension = extension
        super().__init__()

    def get_data(self):  # pragma: no cover
        return get_data_from_url(self.url)

    def make_geometry(self, feature):  # pragma: no cover
        return json.dumps(feature)

    def scrape(self):
        data = self.get_data()

        if self.extension:
            filename = "%s.%s" % (self.table, self.extension)

            if self.extension == 'json':
                sync_file_to_github(
                    self.council_id,
                    filename,
                    format_json(data.decode('utf-8'))
                )
            else:
                sync_file_to_github(self.council_id, filename, data)

        self.store_history(data, self.council_id)
