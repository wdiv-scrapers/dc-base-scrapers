import os
import urllib.request
from copy import deepcopy
from lxml import etree
from common import store_history, truncate, summarise

# hack to override sqlite database filename
# see: https://help.morph.io/t/using-python-3-with-morph-scraperwiki-fork/148
os.environ['SCRAPERWIKI_DATABASE_NAME'] = 'sqlite:///data.sqlite'
import scraperwiki


def make_geometry(xmltree, element):
    geometry = deepcopy(xmltree)
    for e in geometry:
        e.getparent().remove(e)
    geometry.append(deepcopy(xmltree[0]))
    geometry.append(deepcopy(element))
    return etree.tostring(geometry)


def scrape(url, council_id, table, fields, pk):

    with urllib.request.urlopen(url) as response:

        # clear any existing data
        truncate(table)

        # parse xml
        data_str = response.read()
        tree = etree.fromstring(data_str)
        features = tree.findall('{http://www.opengis.net/gml}featureMember')
        print("found %i %s" % (len(features), table))

        for feature in features:

            # create valid GML containing only this feature
            geometry = make_geometry(tree, feature)
            record = {
                'geometry': geometry
            }

            # extract attributes and assemble record
            for attribute in feature[0]:
                if attribute.tag in fields:
                    record[fields[attribute.tag]] = attribute.text

            # save to db
            scraperwiki.sqlite.save(
                unique_keys=[pk],
                data=record,
                table_name=table)
            scraperwiki.sqlite.commit_transactions()

        # print summary
        summarise(table)

        store_history(data_str, table)
