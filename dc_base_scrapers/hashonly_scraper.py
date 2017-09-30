from dc_base_scrapers.common import (
    BaseScraper,
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

    def make_geometry(self, feature):
        return json.dumps(feature)

    def scrape(self):
        data = get_data_from_url(self.url)

        if self.extension:
            sync_file_to_github(
                self.council_id,
                "%s.%s" % (self.table, self.extension),
                data
            )

        self.store_history(data, self.council_id)
