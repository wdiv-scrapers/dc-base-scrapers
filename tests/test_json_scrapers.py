import json
import os
import scraperwiki
import unittest
from arcgis2geojson import arcgis2geojson
from dc_base_scrapers.arcgis_scraper import ArcGisScraper
from dc_base_scrapers.geojson_scraper import GeoJsonScraper
from base import disable_stdout, enable_stdout
from base import ScraperTestCase


class ArcGisScraperStub(ArcGisScraper):

    def get_data(self):
        dirname = os.path.dirname(os.path.abspath(__file__))
        fixture_path = os.path.abspath(os.path.join(dirname, 'fixtures/arcgis.json'))
        data_str = open(fixture_path).read()
        data = json.load(open(fixture_path))
        data_str = bytes(data_str, 'utf-8')
        return (data_str, data)


class GeoJsonScraperStub(GeoJsonScraper):

    def get_data(self):
        # read in the arcgis fixture
        dirname = os.path.dirname(os.path.abspath(__file__))
        fixture_path = os.path.abspath(os.path.join(dirname, 'fixtures/arcgis.json'))
        data = json.load(open(fixture_path))

        # convert it to geojson
        features = []
        for feature in data['features']:
            features.append(arcgis2geojson(feature))
        data = {
            "type": "FeatureCollection",
            "features": features,
        }
        data_str = bytes(json.dumps(data), 'utf-8')

        return (data_str, data)


class JsonScraperTestCase(ScraperTestCase):

    def test_point_conversion(self):
        result = scraperwiki.sqlite.select(" * FROM foo WHERE OBJECTID=1;")
        point = json.loads(result[0]['geometry'])
        self.assertEqual(
            [1.4047961603792665, 51.350812966021017],
            point['geometry']['coordinates']
        )

    def test_area_conversion(self):
        result = scraperwiki.sqlite.select(" * FROM foo WHERE OBJECTID=3;")
        area = json.loads(result[0]['geometry'])
        self.assertEqual([
                [
                    [41.8359375, 71.015625],
                    [21.796875, 36.5625],
                    [56.953125, 33.75],
                    [41.8359375, 71.015625]
                ]
            ],
            area['geometry']['coordinates']
        )


class ArcGisScraperTests(JsonScraperTestCase, unittest.TestCase):

    def run_scraper(self):
        disable_stdout()
        stub = ArcGisScraperStub('foo.bar/baz', 'X01000001', 'utf-8', 'foo')
        stub.scrape()
        enable_stdout()


class GeoJsonScraperTests(JsonScraperTestCase, unittest.TestCase):

    def run_scraper(self):
        disable_stdout()
        stub = GeoJsonScraperStub('foo.bar/baz', 'X01000001', 'utf-8', 'foo')
        stub.scrape()
        enable_stdout()
