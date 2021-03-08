import datetime

from gobcore.model import FIELD
from gobcore.model.quality import QUALITY_CATALOG, get_qa_collection_name


class QualityUpdate():

    CATALOG = QUALITY_CATALOG

    def __init__(self):
        self.source = None
        self.application = None
        self.catalog_name = None
        self.collection_name = None
        self.attribute = None
        self.proces = None

    def _combined_key(self, values, join='_'):
        return join.join([str(value).strip() for value in values if value is not None])

    def get_unique_id(self, issue):
        """
        Each qa issue is uniquely identified by the error, attribute and entity identification
        :param issue:
        :return:
        """
        return self._combined_key([
            self.proces,
            issue.check_id,
            issue.attribute,
            issue.entity_id,
            getattr(issue, FIELD.SEQNR)
        ], '.')

    def get_source(self):
        return self._combined_key([
            self.proces,
            self.source,
            self.application,
            self.attribute])

    def get_header(self, msg_header):
        header = {
            'catalog': QUALITY_CATALOG,
            'collection': get_qa_collection_name(self.catalog_name, self.collection_name),
            'source': self.get_source(),
            'application': self.application,
            'process_id': msg_header.get('process_id'),
            'timestamp': datetime.datetime.utcnow().isoformat(),
            'version': '0.1'
        }
        if msg_header.get('mode'):
            header['mode'] = msg_header['mode']
        return header

    def get_contents(self, issue):
        unique_id = self.get_unique_id(issue)
        return {
            '_source_id': unique_id,
            'meldingnummer': unique_id,
            'code': issue.check_id,
            'proces': self.proces,
            'attribuut': issue.attribute,
            'identificatie': issue.entity_id,
            'volgnummer': getattr(issue, FIELD.SEQNR),
            'begin_geldigheid': getattr(issue, FIELD.START_VALIDITY),
            'eind_geldigheid': getattr(issue, FIELD.END_VALIDITY),
            'betwijfelde_waarde': str(issue.value),
            'onderbouwing': issue.get_explanation(),
            'voorgestelde_waarde': None
        }
