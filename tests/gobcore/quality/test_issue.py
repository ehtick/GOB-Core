from unittest import TestCase
from unittest.mock import MagicMock, patch, ANY

import datetime

from gobcore.model import FIELD
from gobcore.quality.issue import QA_LEVEL, Issue, IssueException, log_issue

class Mock_QA_CHECK():
    any_check = {
        'id': 'any check'
    }

@patch("gobcore.quality.issue.QA_CHECK", Mock_QA_CHECK)
class TestIssue(TestCase):

    def test_issue(self):
        entity = {
            'id': 'any id',
            'attr': 'any attr'
        }

        issue = Issue({'id': 'any_check'}, entity, 'id', 'attr')
        self.assertEqual(issue.check, {'id': 'any_check'})
        self.assertEqual(issue.entity_id_attribute, 'id')
        self.assertEqual(issue.entity_id, entity['id'])
        self.assertIsNone(getattr(issue, FIELD.SEQNR))
        self.assertEqual(issue.attribute, 'attr')
        self.assertEqual(issue.value, entity['attr'])
        self.assertIsNone(issue.compared_to)
        self.assertIsNone(issue.compared_to_value)

        issue = Issue({'id': 'any_check'}, entity, None, 'attr')
        self.assertEqual(issue.entity_id_attribute, Issue._DEFAULT_ENTITY_ID)
        self.assertIsNone(issue.entity_id)

        entity[FIELD.SEQNR] = 'any seqnr'
        issue = Issue({'id': 'any_check'}, entity, None, 'attr')
        self.assertEqual(getattr(issue, FIELD.SEQNR), 'any seqnr')

        issue = Issue({'id': 'any_check'}, entity, None, 'attr', 'other attr')
        self.assertEqual(issue.compared_to, 'other attr')
        self.assertIsNone(issue.compared_to_value)

        entity['other attr'] = 'any other value'
        issue = Issue({'id': 'any_check'}, entity, None, 'attr', 'other attr')
        self.assertEqual(issue.compared_to, 'other attr')
        self.assertEqual(issue.compared_to_value, 'any other value')

    def test_issue_fails(self):
        entity = {
            'id': 'any id',
            'attr': 'any attr'
        }
        with self.assertRaises(IssueException):
            issue = Issue({}, entity, 'id', 'attr')

    def test_get_value(self):
        entity = {
            'id': 'any id',
            'attr': 'any attr'
        }
        issue = Issue({'id': 'any_check'}, entity, 'id', 'attr')

        entity['x'] = None
        result = issue._get_value(entity, 'x')
        self.assertIsNone(result)

        for value in [1, True, "s", 2.0]:
            entity['x'] = value
            result = issue._get_value(entity, 'x')
            self.assertEqual(result, value)

        entity['x'] = datetime.datetime.now()
        result = issue._get_value(entity, 'x')
        self.assertTrue(isinstance(result, str))

    def test_format_value(self):
        entity = {
            'id': 'any id',
            'attr': 'any attr'
        }
        issue = Issue({'id': 'any_check'}, entity, 'id', 'attr')

        result = issue._format_value(None)
        self.assertEqual(result, Issue._NO_VALUE)

        result = issue._format_value(1)
        self.assertEqual(result, "1")

    def test_set_attribute(self):
        entity = {
            'id': 'any id',
            'attr': 'any attr'
        }
        issue = Issue({'id': 'any_check'}, entity, 'id', 'attr')
        issue.set_attribute('anykey', 'anyvalue')
        self.assertEqual(issue.anykey, "anyvalue")
        issue.set_attribute('anykey', 'othervalue', overwrite=False)
        self.assertEqual(issue.anykey, "anyvalue")

    def test_msg(self):
        entity = {
            'id': 'any id',
            'attr': 'any attr'
        }
        check = {
            'id': 'any_check',
            'msg': 'any msg'
        }
        issue = Issue(check, entity, 'id', 'attr')

        result = issue.msg()
        self.assertEqual(result, "attr: any msg")

        issue = Issue(check, entity, 'id', 'attr', 'any compared to')
        result = issue.msg()
        self.assertEqual(result, "attr: any msg any compared to")

    def test_log_args(self):
        entity = {
            'id': 'any id',
            'attr': 'any attr'
        }
        check = {
            'id': 'any_check',
            'msg': 'any msg'
        }
        issue = Issue(check, entity, 'id', 'attr')

        result = issue.log_args()
        self.assertEqual(result, {
            'id': 'attr: any msg',
            'data': {
                'id': 'any id',
                FIELD.SEQNR: None,
                'attr': 'any attr'
            }})

        issue = Issue(check, entity, 'id', 'attr', 'any compared to', 'any compare to value')

        result = issue.log_args()
        self.assertEqual(result, {
            'id': 'attr: any msg any compared to',
            'data': {
                'id': 'any id',
                FIELD.SEQNR: None,
                'attr': 'any attr',
                'any compared to': 'any compare to value'
            }})

        result = issue.log_args(any_key="any value")
        self.assertEqual(result, {
            'id': 'attr: any msg any compared to',
            'data': {
                'id': 'any id',
                FIELD.SEQNR: None,
                'attr': 'any attr',
                'any compared to': 'any compare to value',
                'any_key': 'any value'
            }})

    def test_contents(self):
        entity = {
            'id': 'any id',
            'attr': 'any attr',
            FIELD.SEQNR: 'any seqnr',
            'compared to': 'any compared to value'
        }
        check = {
            'id': 'any_check',
            'msg': 'any msg'
        }
        issue = Issue(check, entity, 'id', 'attr', 'compared to')

        result = issue.contents()
        self.assertEqual(result, {
            'check': 'any_check',
            'id': 'any id',
            'volgnummer': 'any seqnr',
            'attribute': 'attr',
            'value': 'any attr',
            'compared_to': 'compared to',
            'compared_to_value': 'any compared to value',
            'key': 'any_check......attr.any id.any seqnr'
        })

    @patch("gobcore.quality.issue.IssuePublisher")
    def test_log_issue(self, mock_issue_publisher):
        entity = {
            'id': 'any id',
            'attr': 'any attr'
        }
        issue = Issue({'id': 'any_check', 'msg': 'any msg'}, entity, 'id', 'attr')
        mock_logger = MagicMock()
        mock_logger.get_attribute = lambda attr: f"any {attr}"
        mock_logger.get_name.return_value = "any name"
        log_issue(mock_logger, QA_LEVEL.INFO, issue)
        contents = {
            'check': 'any_check',
            'id': 'any id',
            'volgnummer': None,
            'attribute': 'attr',
            'value': 'any attr',
            'compared_to': None,
            'compared_to_value': None,
            'source': 'any source',
            'application': 'any application',
            'catalogue': 'any catalogue',
            'entity': 'any entity',
            'process': 'any name',
            'key': ANY
        }
        mock_issue_publisher.return_value.publish.assert_called_with(contents)
