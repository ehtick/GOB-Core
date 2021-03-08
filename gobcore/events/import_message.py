"""GOB Header

The GOB Header holds the header values of a workflow message.

Todo: Name of the module (message) should correspond to the contents of the file (GOBHeader)

"""


class MessageMetaData():

    def __init__(self, header):
        self._header = header

    @property
    def source(self):
        return self._header['source']

    @property
    def application(self):
        return self._header['application']

    @property
    def timestamp(self):
        return self._header['timestamp']

    @property
    def catalog(self):
        return self._header['catalog']

    @property
    def collection(self):
        return self._header['collection']

    @property
    def version(self):
        return self._header['version']

    @property
    def process_id(self):
        return self._header['process_id']

    @property
    def model(self):
        return {}

    @property
    def as_header(self):
        return {
            "source": self.source,
            "application": self.application,
            "timestamp": self.timestamp,
            "catalog": self.catalog,
            "collection": self.collection,
            "version": self.version,
            "process_id": self.process_id,
            "model": self.model
        }


class ImportMessage():

    def __init__(self, msg):
        self._metadata = MessageMetaData(msg["header"])

        self._summary = msg["summary"]
        self._contents = msg["contents"]

    @property
    def metadata(self):
        return self._metadata

    @property
    def summary(self):
        return self._summary

    @property
    def contents(self):
        return self._contents

    @classmethod
    def create_import_message(cls, header, summary, contents):
        return {
            "header": header,
            "summary": summary,
            "contents": contents,
        }

# Todo, version should be a part of gob_model?
