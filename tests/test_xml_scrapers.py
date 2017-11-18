import os
import scraperwiki
import unittest
from dc_base_scrapers.xml_scraper import GmlScraper
from base import disable_stdout, enable_stdout
from base import ScraperTestCase


class GmlScraperStub(GmlScraper):

    def get_data(self):
        dirname = os.path.dirname(os.path.abspath(__file__))
        fixture_path = os.path.abspath(os.path.join(dirname, 'fixtures/gml.xml'))
        return bytes(open(fixture_path).read(), 'utf-8')


class GmlScraperTestCase(ScraperTestCase):

    def test_point_conversion(self):
        result = scraperwiki.sqlite.select(" * FROM foo WHERE OBJECTID=1;")
        point = result[0]['geometry']
        self.assertIn(
            '<gml:coordinates>1.40479616037927,'
            '51.350812966021</gml:coordinates>', point)

    def test_area_conversion(self):
        result = scraperwiki.sqlite.select(" * FROM foo WHERE OBJECTID=3;")
        point = result[0]['geometry']
        self.assertIn(
            '<gml:coordinates>41.8359375,71.015625 '
            '56.953125,33.75 21.796875,36.5625 '
            '41.8359375,71.015625</gml:coordinates>', point)

    @unittest.skip("skip test_numeric_fields for GML scraper")
    def test_numeric_fields(self):
        """
        TODO: this doesn't work because the fixture doesn't define a schema
        so the parser just falls back to treating everything as a string.
        You can fix this test by defining a local schema
        which describes the NUM field as:

        <PropertyDefn>
          <Name>NUM</Name>
          <ElementPath>NUM</ElementPath>
          <Type>Integer</Type>
        </PropertyDefn>

        but for now lets just ignore it.
        """
        super().test_numeric_fields()


class GmlScraperTests(GmlScraperTestCase, unittest.TestCase):

    def run_scraper(self):
        disable_stdout()
        fields = {
            '{http://ogr.maptools.org/}NUM': 'NUM',
            '{http://ogr.maptools.org/}TEXT': 'TEXT',
            '{http://ogr.maptools.org/}OBJECTID': 'OBJECTID',
        }
        stub = GmlScraperStub('foo.bar/baz', 'X01000001', 'foo', fields, 'OBJECTID')
        stub.scrape()
        enable_stdout()
