import os
import scraperwiki
import unittest
from base import drop_tables, unset_env_vars
from dc_base_scrapers.hashonly_scraper import HashOnlyScraper


class HashOnlyScraperTests(unittest.TestCase):

    def setUp(self):
        unset_env_vars()

    def tearDown(self):
        drop_tables()

    def test_same(self):
        scraper = HashOnlyScraper('foo.bar/baz', 'X01000001', 'foo')

        # monkey patch get_data() to return a known value
        scraper.get_data = lambda: bytes('foo', 'utf-8')

        # run scraper twice
        scraper.scrape()
        scraper.scrape()

        result = scraperwiki.sqlite.select(" * FROM history;")
        # 2 rows should have been inserted
        self.assertEqual(2, len(result))
        # ensure the content hashes for the 2 runs are equal
        self.assertEqual(result[0]['content_hash'], result[1]['content_hash'])

    def test_different(self):
        scraper = HashOnlyScraper('foo.bar/baz', 'X01000001', 'foo')

        # monkey patch get_data() to return a known value
        scraper.get_data = lambda: bytes('foo', 'utf-8')
        scraper.scrape()

        # monkey patch get_data() to return a different value
        # before we run the scraper a second time
        scraper.get_data = lambda: bytes('bar', 'utf-8')
        scraper.scrape()

        result = scraperwiki.sqlite.select(" * FROM history;")
        # 2 rows should have been inserted
        self.assertEqual(2, len(result))
        # ensure the content hashes for the 2 runs are different
        self.assertNotEqual(result[0]['content_hash'], result[1]['content_hash'])
