from unittest import TestCase
from unittest.mock import patch, MagicMock, call

from gobcore.workflow.start_commands import StartCommand, StartCommandArgument, StartCommands, NoSuchCommandException, InvalidArgumentsException


class TestStartCommandArgument(TestCase):

    def test_init(self):
        init_args = {
            'name': 'some name',
            'description': 'some description',
            'required': True,
            'default': 'the default',
            'choices': ['A', 'B', 'C']
        }

        sca = StartCommandArgument(init_args)

        for k, v in init_args.items():
            self.assertEqual(v, getattr(sca, k))

    def test_init_minimal(self):
        expected = {
            'name': 'some name',
            'description': '',
            'required': False,
            'default': None,
            'choices': None,
        }

        sca = StartCommandArgument({'name': 'some name'})

        for k, v in expected.items():
            self.assertEqual(v, getattr(sca, k))

    def test_missing_name(self):
        with self.assertRaises(AssertionError):
            StartCommandArgument({})


class TestStartCommand(TestCase):

    @patch("gobcore.workflow.start_commands.StartCommandArgument")
    def test_init(self, mock_argument):
        init_args = {
            'description': 'some description',
            'workflow': 'some workflow',
            'start_step': 'some start step'
        }

        start_command = StartCommand('some name', init_args)

        self.assertEqual(start_command.name, 'some name')
        for k, v in init_args.items():
            self.assertEqual(v, getattr(start_command, k))

    def test_init_minimal(self):
        expected_args = {
            'name': 'some name',
            'description': '',
            'workflow': 'some workflow',
            'start_step': None,
            'args': [],
        }

        start_command = StartCommand('some name', {'workflow': 'some workflow'})

        for k, v in expected_args.items():
            self.assertEqual(v, getattr(start_command, k))

    def test_init_missing_workflow(self):
        with self.assertRaises(AssertionError):
            StartCommand('some name', {})

    @patch("gobcore.workflow.start_commands.StartCommandArgument")
    def test_init_args(self, mock_argument):
        init_args = {
            'workflow': 'some workflow',
            'args': ['arg1', 'arg2']
        }

        StartCommand('some name', init_args)

        mock_argument.assert_has_calls([
            call('arg1'),
            call('arg2'),
        ])

    def test_validate_arguments(self):
        init_args = {
            'args': [
                {
                    'name': 'arg1',
                }, {
                    'name': 'arg2',
                }
            ],
            'workflow': 'some workflow'

        }

        args_to_validate = {
            'arg1': 'something',
        }

        start_command = StartCommand('some name', init_args)

        # Default case.
        start_command.validate_arguments(args_to_validate)

        start_command.args = [StartCommandArgument({
            'name': 'arg1',
            'required': True,
        }), StartCommandArgument({
            'name': 'arg2',
            'required': True,
        })]

        # Missing required argument
        with self.assertRaisesRegex(InvalidArgumentsException, 'Argument arg2 is required'):
            start_command.validate_arguments(args_to_validate)

        args_to_validate = {
            'arg1': 'something',
            'arg2': ''
        }

        # Present but empty required argument
        with self.assertRaisesRegex(InvalidArgumentsException, 'Argument arg2 is required'):
            start_command.validate_arguments(args_to_validate)

        start_command.args = [StartCommandArgument({
            'name': 'arg1',
            'choices': ['c1', 'c2']
        })]

        # Invalid choice
        with self.assertRaisesRegex(InvalidArgumentsException, 'Argument arg1 must be one of'):
            start_command.validate_arguments(args_to_validate)

        args_to_validate = {
            'arg1': 'c1'
        }

        # Valid choice
        start_command.validate_arguments(args_to_validate)


@patch("gobcore.workflow.start_commands.StartCommand")
@patch("builtins.open")
@patch("gobcore.workflow.start_commands.json.load")
class TestStartCommands(TestCase):

    def test_init_load_json(self, mock_load, mock_open, mock_start_command):
        StartCommands()

    def test_init(self, mock_load, mock_open, mock_start_command):
        mock_load.return_value = {
            "command_a": {
                "key": "value",
                "key2": "value2",
            },
            "command_b": {
                "key3": "value3",
                "key4": "value4",
            }
        }
        StartCommands()

        mock_load.assert_called_with(mock_open.return_value.__enter__.return_value)
        mock_start_command.assert_has_calls([
            call("command_a", mock_load.return_value["command_a"]),
            call("command_b", mock_load.return_value["command_b"]),
        ])

    def test_get(self, mock_load, mock_open, mock_start_command):
        start_commands = StartCommands()
        start_commands.commands['command_a'] = 'some command'

        self.assertEqual('some command', start_commands.get('command_a'))

        with self.assertRaises(NoSuchCommandException):
            start_commands.get('nonexistent')

    def test_get_all(self, mock_load, mock_open, mock_start_command):
        start_commands = StartCommands()
        start_commands.commands = MagicMock()

        self.assertEqual(start_commands.commands, start_commands.get_all())
