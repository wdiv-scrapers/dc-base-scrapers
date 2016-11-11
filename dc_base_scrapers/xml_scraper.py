import abc
from copy import deepcopy
from lxml import etree
from dc_base_scrapers.common import (
    BaseScraper,
    get_data_from_url,
    save,
    summarise,
    truncate,
)


class XmlScraper(BaseScraper, metaclass=abc.ABCMeta):

    def __init__(self, url, council_id, table, fields, pk, store_raw_data=False):
        self.url = url
        self.council_id = council_id
        self.table = table
        self.fields = fields
        self.pk = pk
        self.store_raw_data = store_raw_data
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

            record = {
                'council_id': self.council_id,
                'geometry': self.make_geometry(tree, feature),
            }

            # extract attributes and assemble record
            for attribute in feature[0]:
                if attribute.tag in self.fields:
                    if isinstance(attribute.text, str):
                        record[self.fields[attribute.tag]] = attribute.text.strip()
                    else:
                        record[self.fields[attribute.tag]] = attribute.text

            # save to db
            save(self.pk, record, self.table)

        # print summary
        summarise(self.table)

        self.store_history(data_str, self.council_id)


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
