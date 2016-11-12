from dc_base_scrapers.common import (
    BaseScraper,
    get_data_from_url
)


class HashOnlyScraper(BaseScraper):

    def __init__(self, url, council_id, table):
        self.url = url
        self.council_id = council_id
        self.table = table
        super().__init__()

    def make_geometry(self, feature):
        return json.dumps(feature)

    def scrape(self):
        data = get_data_from_url(self.url)
        self.store_history(data, self.council_id)
