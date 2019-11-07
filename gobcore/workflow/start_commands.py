import json
import os
from gobcore.exceptions import GOBException

START_COMMANDS_CONFIG = 'start_commands.json'


class NoSuchCommandException(GOBException):
    pass


class InvalidArgumentsException(GOBException):
    pass


class StartCommandArgument:
    name: str = ''
    description: str = ''
    required: bool = False
    default: str = None
    choices: list = None

    def __init__(self, config: dict):
        assert 'name' in config

        self.name = config['name']
        self.description = config.get('description', '')
        self.required = config.get('required', False)
        self.named = config.get('named', False)
        self.default = config.get('default')
        self.choices = config.get('choices')


class StartCommand:
    name: str = ''
    description: str = ''
    args: list = []
    workflow: str = None
    start_step: str = None

    def __init__(self, command_name: str, command_config: dict):
        assert 'workflow' in command_config

        self.name = command_name
        self.description = command_config.get('description', '')
        self.workflow = command_config['workflow']
        self.start_step = command_config.get('start_step')
        self.args = []

        for arg in command_config.get('args', []):
            self.args.append(StartCommandArgument(arg))

    def validate_arguments(self, arguments: dict):
        for arg in self.args:
            input_value = arguments[arg.name] if arg.name in arguments else None

            if arg.required and not input_value:
                raise InvalidArgumentsException(f"Argument {arg.name} is required")

            if arg.choices and input_value not in arg.choices:
                raise InvalidArgumentsException(f"Argument {arg.name} must be one of {','.join(arg.choices)}")


class StartCommands:
    commands: dict = {}

    def __init__(self):
        file_location = os.path.join(os.path.abspath(os.path.dirname(__file__)), START_COMMANDS_CONFIG)

        with open(file_location) as f:
            config = json.load(f)

        for command_name, command_config in config.items():
            self.commands[command_name] = StartCommand(command_name, command_config)

    def get(self, name: str):
        if name not in self.commands:
            raise NoSuchCommandException()

        return self.commands[name]

    def get_all(self):
        return self.commands
