import scraperwiki
import unittest
from dc_base_scrapers.common import dump_table_to_json


class DumpTableTests(unittest.TestCase):

    def test_dump_table(self):
        # create tables with same columns in different order
        scraperwiki.sqlite.execute("""CREATE TABLE foo (
            b TEXT,
            a INT,
            c TEXT
        );""")
        scraperwiki.sqlite.execute("""CREATE TABLE bar (
            c TEXT,
            b TEXT,
            a INT
        );""")

        # insert same content differently ordered
        foo_records = [
            {'a': 2, 'b': 'foo', 'c': 'foo'},
            {'a': 1, 'b': 'foo', 'c': 'foo'},
            {'a': 3, 'b': 'foo', 'c': 'foo'},
        ]
        for rec in foo_records:
            scraperwiki.sqlite.save(unique_keys='a', table_name='foo', data=rec)
            scraperwiki.sqlite.commit_transactions()

        bar_records = [
            {'a': 2, 'b': 'foo', 'c': 'foo'},
            {'a': 3, 'b': 'foo', 'c': 'foo'},
            {'a': 1, 'b': 'foo', 'c': 'foo'},
        ]
        for rec in bar_records:
            scraperwiki.sqlite.save(unique_keys='a', table_name='bar', data=rec)
            scraperwiki.sqlite.commit_transactions()

        # check that json representation is consistent
        foo_json = dump_table_to_json('foo', 'a')
        bar_json = dump_table_to_json('bar', 'a')

        self.assertEqual(foo_json, bar_json)
