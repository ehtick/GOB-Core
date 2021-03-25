import datetime
import decimal
import json

from gobcore.typesystem.gob_types import GOBType


class GobTypeJSONEncoder(json.JSONEncoder):
    """Extension of the json.JSONEncoder to help with encoding GOB Types into native JSON

    Use as follows:

        import json

        gob_type = Point.from_values(x=1, y=2)
        json.dumps(gob_type, cls=GobTypeJSONEncoder)

    GOB.Geo-types will return geojson
    """

    def default(self, obj):
        if isinstance(obj, GOBType):
            return json.loads(obj.json)

        if isinstance(obj, decimal.Decimal):
            return json.loads(str(obj))

        if type(obj) is datetime.date:
            return obj.isoformat()

        if type(obj) is datetime.datetime:
            value = obj.isoformat()
            if len(value) == len('YYYY-MM-DDTHH:MM:SS'):
                # Add missing microseconds
                value += '.000000'
            return value
        return super().default(obj)
