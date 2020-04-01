import datetime

from gobcore.model import FIELD


class Issue():
    """
    Data issue class

    """
    _DEFAULT_ENTITY_ID = 'identificatie'
    _NO_VALUE = '<<NO VALUE>>'

    def __init__(self, check, entity, id_attribute, attribute, compared_to=None, compared_to_value=None):
        self.check = check

        # Entity id and sequence number
        self.entity_id_attribute = id_attribute or self._DEFAULT_ENTITY_ID
        self.entity_id = self._get_value(entity, self.entity_id_attribute)
        setattr(self, FIELD.SEQNR, self._get_value(entity, FIELD.SEQNR))

        # Concerned attribute and value
        self.attribute = attribute
        self.value = self._get_value(entity, attribute)

        # Any concerned other attribute
        self.compared_to = compared_to
        self.compared_to_value = compared_to_value
        if compared_to and compared_to_value is None:
            # Default other attribute value is get its value from the entity itself
            self.compared_to_value = self._get_value(entity, self.compared_to)

    def _get_value(self, entity, attribute):
        """
        Gets the value of an entity attribute

        :param entity:
        :param attribute:
        :return:
        """
        # Allow None values
        value = entity.get(attribute)
        if isinstance(value, datetime.date):
            # Dates are iso formatted
            value = value.isoformat()
        return value

    def _format_value(self, value):
        """
        Returns the formatted value.
        Explicitly format None values

        :param value:
        :return:
        """
        return self._NO_VALUE if value is None else str(value)

    def msg(self):
        """
        Return a message that describes the issue at a general level.
        No entity values are included in the message, only attribute names and static strings

        :return:
        """
        msg = f"{self.attribute}: {self.check['msg']}"
        if self.compared_to:
            msg += f" {self.compared_to}"
        return msg

    def log_args(self, **kwargs):
        """
        Convert the issue into arguments that are suitable to add in a log message

        :param kwargs:
        :return:
        """
        args = {
            'id': self.msg(),
            'data': {
                self.entity_id_attribute: self.entity_id,
                self.attribute: self._format_value(self.value),
                **kwargs
            }
        }
        if self.compared_to:
            args['data'][self.compared_to] = self._format_value(self.compared_to_value)
        return args

    def contents(self):
        """
        Return the issue contents as a dictionary

        :return:
        """
        return {
            'check': self.check['id'],
            'id': self.entity_id,
            FIELD.SEQNR: getattr(self, FIELD.SEQNR),
            'attribute': self.attribute,
            'value': self.value,
            'compared_to': self.compared_to,
            'compared_to_value': self.compared_to_value
        }
