import json
import os

from collections import defaultdict

from gobcore.model import GOBModel


class GOBSources():
    _relations = None

    def __init__(self):
        if GOBSources._relations:
            return

        path = os.path.join(os.path.dirname(__file__), 'gobsources.json')
        with open(path) as file:
            data = json.load(file)

        gob_model = GOBModel()

        GOBSources._relations = defaultdict(lambda: defaultdict(list))

        # Extract references for easy access in API
        for source_name, source in data.items():
            self._extract_relations(gob_model, source_name, source)

    def _extract_relations(self, gob_model, source_name, source):
        for catalog_name, catalog in gob_model.get_catalogs().items():
            for collection_name, collection in catalog['collections'].items():
                for field_name, spec in collection['references'].items():
                    field_relation = self._get_field_relation(
                        source,
                        catalog_name,
                        collection_name,
                        field_name
                    )
                    if field_relation:
                        relation = {
                            'source': source_name,
                            'catalog': catalog_name,
                            'collection': collection_name,
                            'field_name': field_name,
                            'type': spec['type'],
                            **field_relation
                        }
                        # Store the relation for the catalog - collection
                        GOBSources._relations[catalog_name][collection_name].append(relation)

    def _get_field_relation(self, source, catalog_name, collection_name, field_name):
        try:
            relation = source[catalog_name][collection_name][field_name]
        except KeyError:
            return {}
        else:
            return relation

    def get_field_relations(self, catalog_name, collection_name, field_name):
        """
        Get all the relations for the specified field in the given catalog - collection

        Not that more field relations may exist because multiple applications may
        be source for the field.

        :param catalog_name:
        :param collection_name:
        :param field_name:
        :return:
        """
        try:
            relations = self.get_relations(catalog_name, collection_name)
            return [relation for relation in relations if relation["field_name"] == field_name]
        except KeyError:
            return []

    def get_relations(self, catalog_name, collection_name):
        return GOBSources._relations[catalog_name][collection_name]
