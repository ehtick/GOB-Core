import datetime

from gobcore.model import FIELD


class KwaliteitBevinding():

    def __init__(self, issues):
        self.issues = issues if isinstance(issues, list) else [issues]
        self.source = None
        self.application = None
        self.catalogue = None
        self.entity = None
        self.proces = None

    def get_unique_id(self, issue):
        key_values = [issue.check_id,
                      self.proces,
                      self.source,
                      self.application,
                      self.catalogue,
                      self.entity,
                      issue.attribute,
                      issue.entity_id,
                      getattr(issue, FIELD.SEQNR)]
        return ".".join([value for value in key_values if value])

    def get_header(self):
        return {
            'catalogue': "kwaliteit",
            'entity': "bevindingen",
            'source': self.source,
            'application': self.application,
            'timestamp': datetime.datetime.utcnow().isoformat(),
            'version': '0.1'
        }

    def get_contents(self, issue):
        unique_id = self.get_unique_id(issue)
        return {
            '_source_id': f"{self.source}.{self.application}.{unique_id}",
            'meldingnummer': unique_id,
            'code': issue.check_id,
            'proces': self.proces,
            'registratie': self.catalogue,
            'objectklasse': self.entity,
            'attribuut': issue.attribute,
            'identificatie': issue.entity_id,
            'volgnummer': getattr(issue, FIELD.SEQNR),
            'betwijfelde_waarde': issue.value,
            'onderbouwing': issue.get_explanation(),
            'voorgestelde_waarde': None
        }

    def get_msg(self):
        return {
            'header': self.get_header(),
            'contents': [self.get_contents(issue) for issue in self.issues],
            'summary': {
                'num_records': len(self.issues)
            }
        }
