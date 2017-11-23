import json
import os
import scraperwiki
import unittest
from base import (
    disable_stdout, enable_stdout, drop_tables, unset_env_vars)
from dc_base_scrapers.ckan_scraper import CkanScraper


class CkanScraperStub(CkanScraper):

    def get_data(self):
        dirname = os.path.dirname(os.path.abspath(__file__))
        fixture_path = os.path.abspath(os.path.join(dirname, 'fixtures/ckan.json'))
        data_str = open(fixture_path).read()
        data = json.load(open(fixture_path))
        data_str = bytes(data_str, 'utf-8')
        return (data_str, data)


class CkanScraperTests(unittest.TestCase):

    def setUp(self):
        unset_env_vars()

    def tearDown(self):
        drop_tables()

    def test_scraper(self):
        disable_stdout()
        stub = CkanScraperStub('foo.bar/', 'X01000001', 'baz', 'csv', [], 'utf-8')
        stub.scrape()
        enable_stdout()

        # should have inserted one row for each resource
        result = scraperwiki.sqlite.select(" * FROM resources;")
        self.assertEqual(2, len(result))
        self.assertTrue("created" in result[0])
        self.assertTrue("format" in result[0])
        self.assertTrue("revision_id" in result[0])
        self.assertTrue("url" in result[0])
        self.assertFalse("description" in result[0])

    def test_return_value(self):
        disable_stdout()
        stub = CkanScraperStub('foo.bar/', 'X01000001', 'baz', 'csv', [], 'utf-8')
        url = stub.scrape()
        enable_stdout()
        self.assertEqual("https://foo.bar/baz.csv", url)

    def test_extra_fields(self):
        disable_stdout()
        stub = CkanScraperStub('foo.bar/', 'X01000001', 'baz', 'csv', ['description'], 'utf-8')
        stub.scrape()
        enable_stdout()

        result = scraperwiki.sqlite.select(" * FROM resources;")
        self.assertTrue("description" in result[0])

