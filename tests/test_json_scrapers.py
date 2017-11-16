import abc
import json
import os
import sys
import scraperwiki
import unittest
from arcgis2geojson import arcgis2geojson
from dc_base_scrapers.arcgis_scraper import ArcGisScraper
from dc_base_scrapers.geojson_scraper import GeoJsonScraper


def disable_stdout():
    sys.stdout = open(os.devnull, 'w')

def enable_stdout():
    sys.stdout = sys.__stdout__


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


class JsonScraperTestCase(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def run_scraper(self):
        pass

    def setUp(self):

        # ensure env vars are definitely not set
        if 'MORPH_GITHUB_REPO' in os.environ:
            del(os.environ['MORPH_GITHUB_REPO'])
        if 'MORPH_GITHUB_USERNAME' in os.environ:
            del(os.environ['MORPH_GITHUB_USERNAME'])
        if 'MORPH_GITHUB_EMAIL' in os.environ:
            del(os.environ['MORPH_GITHUB_EMAIL'])
        if 'MORPH_GITHUB_API_KEY' in os.environ:
            del(os.environ['MORPH_GITHUB_API_KEY'])

        self.run_scraper()

    def tearDown(self):
        scraperwiki.sqlite.execute("DROP TABLE IF EXISTS foo;")
        scraperwiki.sqlite.execute("DROP TABLE IF EXISTS history;")
        scraperwiki.sqlite.commit_transactions()


    def test_columns(self):
        result = scraperwiki.sqlite.execute("PRAGMA table_info(foo);")
        columns = [col[1] for col in result['data']]

        # columns from fixture
        self.assertTrue('TEXT' in columns)
        self.assertTrue('NUM' in columns)
        self.assertTrue('OBJECTID' in columns)

        # implicit cols
        self.assertTrue('council_id' in columns)
        self.assertTrue('geometry' in columns)

    def test_all_objects_inserted(self):
        result = scraperwiki.sqlite.select(" * FROM foo ORDER BY OBJECTID;")
        self.assertEqual(3, len(result))

    def test_numeric_fields(self):
        result = scraperwiki.sqlite.select(" * FROM foo ORDER BY OBJECTID;")
        self.assertEqual(12,    result[0]['NUM'])
        self.assertEqual(60.71, result[2]['NUM'])
        for row in result:
            self.assertTrue(isinstance(row['NUM'], (int, float)))

    def test_text_fields(self):
        result = scraperwiki.sqlite.select(" * FROM foo ORDER BY OBJECTID;")
        self.assertEqual('foo', result[0]['TEXT'])
        for row in result:
            self.assertTrue(isinstance(row['TEXT'], str))

    def test_council_field(self):
        result = scraperwiki.sqlite.select(" * FROM foo ORDER BY OBJECTID;")
        for row in result:
            self.assertEqual('X01000001', row['council_id'])

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
                    [56.953125, 33.75],
                    [21.796875, 36.5625],
                    [41.8359375, 71.015625]
                ]
            ],
            area['geometry']['coordinates']
        )

    def test_history(self):
        # run the scraper a second time
        self.run_scraper()

        result = scraperwiki.sqlite.select(" * FROM history;")

        # 2 rows should have been inserted
        self.assertEqual(2, len(result))

        # ensure the content hashes for the 2 runs are equal
        self.assertEqual(result[0]['content_hash'], result[1]['content_hash'])


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
