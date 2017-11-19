import abc
import os
import sys
import scraperwiki


def disable_stdout():
    sys.stdout = open(os.devnull, 'w')

def enable_stdout():
    sys.stdout = sys.__stdout__

def unset_env_vars():
    # ensure env vars are definitely not set
    if 'MORPH_GITHUB_REPO' in os.environ:
        del(os.environ['MORPH_GITHUB_REPO'])
    if 'MORPH_GITHUB_USERNAME' in os.environ:
        del(os.environ['MORPH_GITHUB_USERNAME'])
    if 'MORPH_GITHUB_EMAIL' in os.environ:
        del(os.environ['MORPH_GITHUB_EMAIL'])
    if 'MORPH_GITHUB_API_KEY' in os.environ:
        del(os.environ['MORPH_GITHUB_API_KEY'])

def drop_tables():
    scraperwiki.sqlite.execute("DROP TABLE IF EXISTS foo;")
    scraperwiki.sqlite.execute("DROP TABLE IF EXISTS history;")
    scraperwiki.sqlite.commit_transactions()


class ScraperTestCase(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def run_scraper(self):
        pass

    def setUp(self):
        unset_env_vars()
        self.run_scraper()

    def tearDown(self):
        drop_tables()

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

    def test_history(self):
        # run the scraper a second time
        self.run_scraper()

        result = scraperwiki.sqlite.select(" * FROM history;")

        # 2 rows should have been inserted
        self.assertEqual(2, len(result))

        # ensure the content hashes for the 2 runs are equal
        self.assertEqual(result[0]['content_hash'], result[1]['content_hash'])


