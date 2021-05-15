import os
import glob
import json

from gobcore.exceptions import GOBException
from gobcore.model.metadata import FIELD
from gobcore.model.metadata import STATE_FIELDS
from gobcore.model.metadata import PRIVATE_META_FIELDS, PUBLIC_META_FIELDS, FIXED_FIELDS
from gobcore.model.relations import get_relations, get_inverse_relations, set_relations, init_relations
from gobcore.model.quality import (
    QUALITY_CATALOG,
    get_quality_assurances,
    init_qualty_assurance,
    set_quality_assurance_collections,
)
from gobcore.model.schema import load_schema, SchemaException
from gobcore.model.ams_schema import get_gob_model


EVENTS_DESCRIPTION = {
    "eventid": "Unique identification of the event, numbered sequentially",
    "timestamp": "Datetime when the event as created",
    "catalogue": "The catalogue in which the entity resides",
    "entity": "The entity to which the event need to be applied",
    "version": "The version of the entity model",
    "action": "Add, change, delete or confirm",
    "source": "The functional source of the entity, e.g. AMSBI",
    "application": "The technical source of the entity, e.g. DIVA",
    "source_id": "The id of the entity in the source",
    "contents": "A json object that holds the contents for the action, the full entity for an Add"
}

EVENTS = {
    "eventid": "GOB.PKInteger",
    "timestamp": "GOB.DateTime",
    "catalogue": "GOB.String",
    "entity": "GOB.String",
    "version": "GOB.String",
    "action": "GOB.String",
    "source": "GOB.String",
    "application": "GOB.String",
    "source_id": "GOB.String",
    "contents": "GOB.JSON"
}


class NotInModelException(Exception):
    pass


class NoSuchCatalogException(NotInModelException):
    pass


class NoSuchCollectionException(NotInModelException):
    pass


class GOBModel():
    inverse_relations = None
    _data = None

    def __init__(self):
        if GOBModel._data is not None:
            # Model is already initialised
            return

        data = {}
        path = os.path.join(os.path.dirname(__file__), 'gobmodel')
        for f in glob.glob(os.path.join(path, '*.json')):
            with open(f) as file:
                data |= json.load(file)

        if os.getenv('DISABLE_TEST_CATALOGUE', False):
            # Default is to include the test catalogue
            # By setting the DISABLE_TEST_CATALOGUE environment variable
            # the test catalogue can be removed
            del data["test_catalogue"]

        GOBModel._data = data
        self._load_schemas()
        data[QUALITY_CATALOG] = get_quality_assurances(self)
        data["rel"] = get_relations(self)

        global_attributes = {
            **PRIVATE_META_FIELDS,
            **PUBLIC_META_FIELDS,
            **FIXED_FIELDS
        }

        # Extract references for easy access in API. Add catalog and collection names to catalog and collection objects
        for catalog_name, catalog in self._data.items():
            catalog['name'] = catalog_name

            for entity_name, model in catalog['collections'].items():
                model['name'] = entity_name
                model['references'] = self._extract_references(model['attributes'])
                model['very_many_references'] = self._extract_very_many_references(model['attributes'])

                model_attributes = model['attributes']
                state_attributes = STATE_FIELDS if self.has_states(catalog_name, entity_name) else {}
                all_attributes = {
                    **state_attributes,
                    **model_attributes
                }

                # Add fields to the GOBModel to be used in database creation and lookups
                model['fields'] = all_attributes

                # Include complete definition, including all global fields
                model['all_fields'] = {
                    **all_attributes,
                    **global_attributes
                }

    def _load_schemas(self):
        """
        Load any external schemas and updates model accordingly

        :return: None
        """
        for catalog_name, catalog in self._data.items():
            for entity_name, model in catalog['collections'].items():
                if model.get('schema') is not None:
                    try:
                        model['attributes'] = load_schema(model['schema'], catalog_name, entity_name)
                    except SchemaException:
                        # Use a fallback scenario as long as the schemas are still in development
                        print(f"ERROR: failed to load schema {model['schema']} for {catalog_name}:{entity_name}")
                        model['attributes'] = model["_attributes"]

    @staticmethod
    def _extract_references(attributes):
        return {field_name: spec for field_name, spec in attributes.items()
                if spec['type'] in ['GOB.Reference', 'GOB.ManyReference', 'GOB.VeryManyReference']}

    @staticmethod
    def _extract_very_many_references(attributes):
        return {field_name: spec for field_name, spec in attributes.items()
                if spec['type'] in ['GOB.VeryManyReference']}

    def get_inverse_relations(self):
        if not self.inverse_relations:
            self.inverse_relations = get_inverse_relations(self)
        return self.inverse_relations

    def get_catalog_names(self):
        return self._data.keys()

    def get_catalogs(self):
        return self._data

    def get_catalog(self, catalog_name):
        return self._data[catalog_name] if catalog_name in self._data else None

    def get_collection_names(self, catalog_name):
        catalog = self.get_catalog(catalog_name)
        return catalog['collections'].keys() if 'collections' in catalog else None

    def get_collections(self, catalog_name):
        catalog = self.get_catalog(catalog_name)
        return catalog['collections'] if catalog and 'collections' in catalog else None

    def get_collection(self, catalog_name, collection_name):
        collections = self.get_collections(catalog_name)
        return collections[collection_name] if collections and collection_name in collections else None

    def get_collection_by_name(self, collection_name):
        """Finds collection only by name. Raises GOBException when multiple collections with name are found. Returns
        (catalog, collection) tuple when success.

        :param collection_name:
        :return:
        """
        collections = []
        catalog = None

        for catalog_name in self.get_catalog_names():
            collection = self.get_collection(catalog_name, collection_name)

            if collection:
                collections.append(collection)
                catalog = catalog_name

        if len(collections) > 1:
            raise GOBException(f"Multiple collections found with name {collection_name}")

        return (catalog, collections[0]) if collections else None

    def get_functional_key_fields(self, catalog_name, collection_name):
        """
        Return the list of fieldnames that functionally identifies an entity

        :param catalog_name: name of the catalog
        :param collection_name: name of the collection
        :return: array of fieldnames
        """
        collection = self.get_collection(catalog_name, collection_name)
        result = [FIELD.SOURCE, collection["entity_id"]]
        if self.has_states(catalog_name, collection_name):
            result.append(FIELD.SEQNR)
        return result

    def get_technical_key_fields(self, catalog_name, collection_name):
        """
        Return the list of fieldnames that technically identifies an entity

        :param catalog_name: name of the catalog
        :param collection_name: name of the collection
        :return: array of fieldnames
        """
        return [FIELD.SOURCE, FIELD.SOURCE_ID]

    def has_states(self, catalog_name, collection_name):
        """
        Tells if a collection has states

        :param catalog_name: name of the catalog
        :param collection_name: name of the collection
        :return: True if the collection has states
        """
        collection = self.get_collection(catalog_name, collection_name)
        return collection.get("has_states") is True

    def get_source_id(self, entity, input_spec):
        """
        Gets the id that uniquely identifies the entity within the source

        :param entity: the entity
        :param input_spec: the input format specification
        :return: the source id
        """
        source_id_field = input_spec['source']['entity_id']
        source_id = str(entity[source_id_field])
        if self.has_states(input_spec['catalogue'], input_spec['entity']):
            # Volgnummer could be a different field in the source entity than FIELD.SEQNR
            try:
                seqnr_field = input_spec['gob_mapping'][FIELD.SEQNR]['source_mapping']
            except KeyError:
                seqnr_field = FIELD.SEQNR

            # Source id + volgnummer is source id
            source_id = f"{source_id}.{entity[seqnr_field]}"
        return source_id

    def get_reference_by_abbreviations(self, catalog_abbreviation, collection_abbreviation):
        for catalog_name, catalog in self._data.items():
            if catalog['abbreviation'] == catalog_abbreviation.upper():
                for collection_name, collection in catalog['collections'].items():
                    if collection['abbreviation'] == collection_abbreviation.upper():
                        return ':'.join([catalog_name, collection_name])

    def get_table_names(self):
        '''
        Helper function to generate all table names
        '''
        table_names = []
        for catalog_name in self._data.keys():
            for collection_name in self.get_collection_names(catalog_name):
                table_names.append(self.get_table_name(catalog_name, collection_name))
        return table_names

    def get_table_name(self, catalog_name, collection_name):
        return f'{catalog_name}_{collection_name}'.lower()

    def get_table_name_from_ref(self, ref):
        """Returns the table name from a reference

        :param ref:
        :return:
        """
        catalog, collection = self.split_ref(ref)
        return self.get_table_name(catalog, collection)

    def split_ref(self, ref) -> tuple:
        """Splits reference into tuple of (catalog_name, collection_name)

        :param ref:
        :return:
        """
        split_res = ref.split(':')

        if len(split_res) != 2 or not all([len(item) > 0 for item in split_res]):
            raise GOBException(f"Invalid reference {ref}")
        return split_res

    def get_catalog_collection_names_from_ref(self, ref):
        return self.split_ref(ref)

    def get_collection_from_ref(self, ref) -> dict:
        """Returns collection ref is referring to

        :param ref:
        :return:
        """
        catalog_name, collection_name = self.split_ref(ref)
        return self.get_collection(catalog_name, collection_name)

    def _split_table_name(self, table_name: str):
        split = [part for part in table_name.split('_') if part]

        if len(split) < 2:
            raise GOBException("Invalid table name")

        return split

    def get_catalog_from_table_name(self, table_name: str):
        """Returns catalog name from table name

        :param table_name:
        :return:
        """
        return self._split_table_name(table_name)[0]

    def get_collection_from_table_name(self, table_name: str):
        """Returns collection name from table name

        :param table_name:
        :return:
        """
        return "_".join(self._split_table_name(table_name)[1:])

    def get_catalog_from_abbr(self, catalog_abbr: str):
        """Returns catalog from abbreviation

        :param catalog_abbr:
        """
        try:
            return [catalog for catalog in self._data.values() if catalog['abbreviation'].lower() == catalog_abbr][0]
        except IndexError:
            raise NoSuchCatalogException(catalog_abbr)

    def get_catalog_collection_from_abbr(self, catalog_abbr: str, collection_abbr: str):
        """Returns catalog and collection

        :param catalog_abbr:
        :param collection_abbr:
        :return:
        """

        catalog = self.get_catalog_from_abbr(catalog_abbr)

        try:
            collection = [collection for collection in catalog['collections'].values()
                          if collection['abbreviation'].lower() == collection_abbr][0]
        except IndexError:
            raise NoSuchCollectionException(collection_abbr)

        return catalog, collection

    @classmethod
    def _get_catalog(cls, catalog_name):
        pass


class LazyDict(dict):
    '''
      Fetches items on request, all
      items will contain a result of
      the get_item function
    '''

    def __init__(self, get_item: callable):
        self.get_item = get_item

    def __get_item(self, item):
        if not dict.__contains__(self, item):
            try:
                value = self.get_item(item)
            except Exception as e:
                print(f'Failed fetching {item} {e}')
                return None
            super(LazyDict, self).__setitem__(item, value)
        return super(LazyDict, self).__getitem__(item)

    def __getitem__(self, item):
        return self.__get_item(item)

    def get(self, item, owner=None):
        self.__get_item(item)


if os.environ.get('USE_AMS_SCHEMA'): # Noqa

    class GOBModel(GOBModel):
        '''
           Very dirty class override, but for now work fine
           can enable or disable AMS model with USE_AMS_SCHEME

           Catalogue fetching should be fteched on request.
        '''
        inverse_relations = None

        _catalogs = None
        _quality_assurance = None
        _relations = None

        def __init__(self):
            if GOBModel._catalogs is not None:
                return
            GOBModel._catalogs = LazyDict(GOBModel._get_catalog)
            GOBModel._quality_assurance = init_qualty_assurance()
            GOBModel._relations = init_relations()

        @classmethod
        def _get_catalog(cls, catalog_name):
            catalog = get_gob_model(catalog_name)
            cls._load_schema(catalog_name, catalog)
            set_quality_assurance_collections(cls._quality_assurance, catalog_name, catalog)
            cls.add_global_attributes(catalog_name, catalog)
            # Relations are an issue...
            # If relations contain refs to external catolog,
            # we need to fetch them also, but we can leave this
            # until we do the relate proces.
            # For now set the relations to None.
            set_relations(cls._relations, cls, catalog_name, catalog)
            return catalog

        def get_catalog(self, catalog_name):
            return self._catalogs[catalog_name]

        def get_collection_names(self, catalog_name):
            return self._catalogs.keys()

        def get_collections(self, catalog_name):
            catalog = self.get_catalog(catalog_name) or {}
            return catalog.get('collections')

        @classmethod
        def get_collection(cls, catalog_name, collection_name):
            print(f'catalog_name=={catalog_name}')
            catalog = cls._catalogs[catalog_name] or {}
            collections = catalog.get('collections', {})
            return collections.get(collection_name)

        @staticmethod
        def _load_schema(catalog_name: str, catalog: dict):
            """
            Load any external schemas and updates model accordingly
            :return: None
            """
            for entity_name, model in catalog['collections'].items():
                if model.get('schema') is not None:
                    try:
                        model['attributes'] = load_schema(model['schema'], catalog_name, entity_name)
                    except SchemaException:
                        # Use a fallback scenario as long as the schemas are still in development
                        print(f"ERROR: failed to load schema {model['schema']} for {catalog_name}:{entity_name}")
                        model['attributes'] = model["_attributes"]

        @classmethod
        def add_global_attributes(cls, catalog_name, catalog):
            global_attributes = {
                **PRIVATE_META_FIELDS,
                **PUBLIC_META_FIELDS,
                **FIXED_FIELDS
            }

            # Extract references for easy access in API. Add catalog and
            # collection names to catalog and collection objects
            catalog['name'] = catalog_name

            for entity_name, model in catalog['collections'].items():
                model['name'] = entity_name
                model['references'] = GOBModel._extract_references(model['attributes'])
                model['very_many_references'] = GOBModel._extract_very_many_references(model['attributes'])

                model_attributes = model['attributes']
                state_attributes = STATE_FIELDS if model.get('has_state') else {}
                all_attributes = {
                    **state_attributes,
                    **model_attributes
                }

                # Add fields to the GOBModel to be used in database creation and lookups
                model['fields'] = all_attributes

                # Include complete definition, including all global fields
                model['all_fields'] = {
                    **all_attributes,
                    **global_attributes
                }
