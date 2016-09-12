import abc
import scraperwiki
from copy import deepcopy
from lxml import etree
from dc_base_scrapers.common import (
    BaseScraper, truncate, summarise, get_data_from_url)


class XmlScraper(BaseScraper, metaclass=abc.ABCMeta):

    def __init__(self, url, council_id, table, fields, pk):
        self.url = url
        self.council_id = council_id
        self.table = table
        self.fields = fields
        self.pk = pk
        super().__init__()

    @abc.abstractmethod
    def make_geometry(self, xmltree, element):
        pass

    @property
    @abc.abstractmethod
    def feature_tag(self):
        pass

    def scrape(self):

        if not isinstance(self.pk, list):
            self.pk = [self.pk]

        # load xml
        data_str = get_data_from_url(self.url)
        tree = etree.fromstring(data_str)
        features = tree.findall(self.feature_tag)
        print("found %i %s" % (len(features), self.table))

        # clear any existing data
        truncate(self.table)

        for feature in features:

            # create valid geometry containing only this feature
            geometry = self.make_geometry(tree, feature)

            record = {
                'council_id': self.council_id,
                'geometry': geometry
            }

            # extract attributes and assemble record
            for attribute in feature[0]:
                if attribute.tag in self.fields:
                    record[self.fields[attribute.tag]] = attribute.text

            # save to db
            scraperwiki.sqlite.save(
                unique_keys=self.pk,
                data=record,
                table_name=self.table)
            scraperwiki.sqlite.commit_transactions()

        # print summary
        summarise(self.table)

        self.store_history(data_str)


class GmlScraper(XmlScraper):

    feature_tag = '{http://www.opengis.net/gml}featureMember'

    def make_geometry(self, xmltree, element):
        geometry = deepcopy(xmltree)
        for e in geometry:
            e.getparent().remove(e)
        geometry.append(deepcopy(xmltree[0]))
        geometry.append(deepcopy(element))
        return etree.tostring(geometry)


class Wfs2Scraper(XmlScraper):

    feature_tag = '{http://www.opengis.net/wfs/2.0}member'

    def make_geometry(self, xmltree, element):
        geometry = deepcopy(xmltree)
        for e in geometry:
            e.getparent().remove(e)
        geometry.append(deepcopy(element))
        return etree.tostring(geometry)
