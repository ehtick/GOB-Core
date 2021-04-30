import math
import urllib
import os
import os.path
import json
import re

from gobcore.typesystem import gob_types
from gobcore.exceptions import GOBTypeException

# Taken from gobmodel.json
CATALOGUE_ABBR = {
    'meetbouten': 'MBN',
    'nap': 'NAP',
    'gebieden': 'GBD',
    'bag': 'BAG',
    'bgt': 'BGT',
    'brk': 'BRK',
    'wkpb': 'WKPB',
    'woz': 'WOZ',
    'hr': 'HR',
}

# Taken from gobmodel.json
COLLECTION_ABBR = {
    'meetbouten': {
        'meetbouten': 'MBT',
        'metingen': 'MTG',
        'referentiepunten': 'RPT',
        'rollagen': 'RLG'},
    'nap': {
        'peilmerken': 'PMK'},
    'gebieden': {
        'bouwblokken': 'BBK',
        'buurten': 'BRT',
        'wijken': 'WIJK',
        'ggwgebieden': 'GGW',
        'ggpgebieden': 'GGP',
        'stadsdelen': 'SDL'},
    'bag': {
        'woonplaatsen': 'WPS',
        'standplaatsen': 'SPS',
        'ligplaatsen': 'LPS',
        'openbareruimtes': 'ORE',
        'nummeraanduidingen': 'NAG',
        'verblijfsobjecten': 'VOT',
        'panden': 'PND',
        'dossiers': 'DSR',
        'brondocumenten': 'BDT',
        'onderzoeken': 'OZK'},
    'bgt': {
        'onderbouw': 'ONW', 'overbouw': 'OVW'},
    'brk': {
        'kadastraleobjecten': 'KOT',
        'zakelijkerechten': 'ZRT',
        'kadastralesubjecten': 'SJT',
        'tenaamstellingen': 'TNG',
        'aantekeningenrechten': 'ART',
        'aantekeningenkadastraleobjecten': 'AKT',
        'stukdelen': 'SDL',
        'aardzakelijkerechten': 'AZT',
        'gemeentes': 'GME',
        'meta': 'META',
        'kadastralesecties': 'KSE',
        'kadastralegemeentecodes': 'KCE',
        'kadastralegemeentes': 'KGE'},
    'wkpb': {
        'beperkingen': 'BPG',
        'dossiers': 'DSR',
        'brondocumenten': 'BDT'},
    'woz': {
        'wozobjecten': 'WOT',
        'wozdeelobjecten': 'WDT'},
    'hr': {
        'maatschappelijkeactiviteiten': 'MAC',
        'locaties': 'LOC',
        'vestigingen': 'VES'}
}


def Ams2GOBString(field: dict):
    format_ = field.get('format', None)
    try:
        gob_type = {
            None: gob_types.String,
            'date': gob_types.Date,
            'date-time': gob_types.DateTime,
            'uri': gob_types.URI,
            'time': gob_types.Time,
            'interval': gob_types.Interval,
        }[format_]
        return {'type': f'GOB.{gob_type.name}'}
    except KeyError:
        raise GOBTypeException(f'Unsupported string type:{format_}')


def _replace(field: dict, item, replace_item, value_func):
    if item in field:
        field[replace_item] = value_func(field[item])
        del(item[field])


def _optional(fields: dict, name: str)-> dict:
    return {name: fields[name]} if name in fields else {}


def Ams2GOBNumber(fields: dict):
    ret = {
        'type': 'GOB.Decimal'
    }
    if 'multipleOf' in fields:
        ret |= {'precision': math.log(float(fields['multipleOf']), 10)}
    return ret


def AMS2type(fields: dict, gob_type: gob_types.GOBType):
    return {
        'type': f'GOB.{gob_type.name}',
    }


def Ams2GOBObject(fields):
    return {
        'type': 'GOB.Reference',
        'ref': fields['relation']
    }


def Ams2GOBArray(fields):
    if 'relation' in fields:
        # identificatie on reference will alwys be used
        if fields['items']['type'] != 'object':
            raise GOBTypeException('relations can only reference objects')
        return {'type': 'GOB.ManyReference', 'ref': fields['relation']}
    # all other arrays are hanlded as json data

    return {'type': 'GOB.JSON'}


def gob_type_by_ref(ref):
    try:
        gob_type = {
            "https://geojson.org/schema/Geometry.json": "GOB.Geomerty",
            "https://geojson.org/schema/Polygon.json": "GOB.Geo.Polygon",
            "https://geojson.org/schema/MultiPolygon.json": "GOB.Geometry",
        }[ref]
    except KeyError:
        raise GOBTypeException(f'Unkown $ref type {ref}')
    return {'type': f'GOB.{gob_type}'}


# For now only support basic types
transform = {
    'string': Ams2GOBString,
    'integer': lambda f: AMS2type(f, gob_types.Integer),
    'boolean': lambda f: AMS2type(f, gob_types.Boolean),
    'object': Ams2GOBObject,
    'number': Ams2GOBNumber,
    'array': Ams2GOBArray,
}

DEFAULT_SCHEMA_REF = 'https://raw.githubusercontent.com/Amsterdam/amsterdam-schema/master/datasets/'
AMS_SCHEMA_REF = os.environ.get('AMS_SCHEME_REF', DEFAULT_SCHEMA_REF)


def to_snake(camel_str: str)->str:
    return re.sub(r'(?<=[a-z])[A-Z]|[A-Z](?=[^A-Z])', r'_\g<0>', camel_str).lower().strip('_')


def _get_gob_attribute(field_name, ams_field: dict):
    if 'type' in ams_field:
        t = transform[ams_field['type']]
        ret = t(ams_field)
    elif '$ref' in ams_field:
        ret = gob_type_by_ref(ams_field['$ref'])
    else:
        raise GOBTypeException(f'Unknown type for field={field_name}')
    ret.update(_optional(ams_field, 'description'))
    return ret


def _get_gob_attributes(properties: dict, has_states: bool):
    # schema is not needed.
    skip_attributes = {'schema', 'volgnummer'} if has_states else {'schema'}
    return {
        to_snake(k): _get_gob_attribute(k, v)
        for (k, v) in properties.items() if k not in skip_attributes
    }


def _get_gob_collection(catalogue: str, ams_table: dict):
    schema = ams_table['schema']
    ret = {}
    identifier = schema.get('identifier')
    has_states = False
    if identifier is not None:
        entity_id = identifier[0] if isinstance(identifier, list) else identifier
        ret['entity_id'] = entity_id
        has_states = True if isinstance(identifier, list) else False

    if has_states:
        ret['has_states'] = True
    try:
        abbreviation = COLLECTION_ABBR[catalogue][ams_table['id']]
    except KeyError:
        abbreviation = ams_table.get('shortname', ams_table['id'])
    ret.update({
        'version': '0.1',
        'abbreviation': abbreviation,
        'attributes': _get_gob_attributes(ams_table['schema']['properties'], has_states),
    })
    return ret


def _get_gob_collections(catalog: str, collections: list):
    return {table['id']: _get_gob_collection(catalog, table) for table in collections}


def ams2gob_model(ams_model: dict):
    catalog = ams_model['id']
    ret = {
        "version": "0.1",
        "abbreviation": CATALOGUE_ABBR[catalog],
        'collections': _get_gob_collections(catalog, ams_model['tables'])
    }
    ret.update(_optional(ams_model, 'description'))
    return ret


def get_ams_model(catalogue):
    url = os.path.join(AMS_SCHEMA_REF, catalogue, catalogue + '.json')
    print(f'Fetching schema from {url}')
    with urllib.request.urlopen(url) as response:
        return json.loads(response.read().decode('utf-8'))


def get_model(catalog):
    return ams2gob_model(get_ams_model(catalog))


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("catalog")
    parser.add_argument('out_file')
    args = parser.parse_args()
    translated_model = get_model(args.catalog)
    with open(args.out_file, 'wb') as f:
        f.write(json.dumps(translated_model, indent=4, ensure_ascii=False).encode('utf-8'))
    gob_model = json.load(open(os.path.join(os.path.dirname(__file__), 'gobmodel.json')))
    gob_catalog = gob_model[args.catalog]
    for entity in gob_catalog['collections'].keys():
        tr_fields = set(translated_model['collections'][entity].keys())
        gob_fields = set(gob_catalog['collections'][entity].keys())
        fields_diff = tr_fields.symmetric_difference(set(gob_fields))
        if fields_diff:
            print(f'catalog={args.catalog} entity={entity} fields_diff={fields_diff}')
        else:
            tr_attr = set(attr for attr in translated_model['collections'][entity]['attributes'])
            gob_attr = set(attr for attr in gob_catalog['collections'][entity]['attributes'])
            attr_diff = tr_attr.symmetric_difference(gob_attr)
            if attr_diff:
                print(f'catalog={args.catalog} entity={entity} attr_diff={attr_diff}')
