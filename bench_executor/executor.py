#!/usr/bin/env python3

import os
import sys
import json
import jsonschema
import importlib
import inspect

METADATA_FILE = 'metadata.json'
SCHEMA_FILE = 'metadata.schema'
DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')

class Executor:
    def __init__(self, main_directory: str, verbose: bool = False):
        self._main_directory = main_directory
        self._schema = {}
        self._resources = None
        self._verbose = verbose

        with open(os.path.join(DATA_DIR, SCHEMA_FILE)) as f:
            self._schema = json.load(f)

    @property
    def resources(self) -> list:
        if self._resources is not None:
            return self._resources
        else:
            self._resources = []

        # Discover all modules to import
        sys.path.append(os.path.dirname(__file__))
        modules = list(filter(lambda x: x.endswith('.py') \
                              and '__init__' not in x \
                              and '__pycache__' not in x,
                              os.listdir(os.path.dirname(__file__))))

        # Discover all classes in each module
        for m in modules:
            module_name = os.path.splitext(m)[0]
            imported_module = importlib.import_module(module_name)
            for name, cls in inspect.getmembers(imported_module, inspect.isclass):
                if name.startswith('_') or name[0].islower():
                    continue

                # Discover all methods and their parameters in each class
                methods = {}
                for method_name, method in filter(lambda x: '__init__' not in x,
                                                  inspect.getmembers(cls, inspect.isfunction)):
                    parameters = inspect.signature(method).parameters
                    methods[method_name] = []
                    for key in parameters.keys():
                        if key == 'self':
                            continue
                        p = parameters[key]
                        required = p.default == inspect.Parameter.empty
                        methods[method_name].append({'name': p.name,
                                                     'required': required})

                if name not in list(filter(lambda x: x['name'], self._resources)):
                    self._resources.append({'name': name, 'commands': methods})

        return self._resources

    def resources_all_names(self) -> list:
        names = []
        for r in self.resources:
            names.append(r['name'])

        if names:
            return names
        else:
            return None

    def resources_all_commands_by_name(self, name: str) -> list:
        commands = []
        for r in filter(lambda x: x['name'] == name, self.resources):
            commands += list(r['commands'].keys())
        if commands:
            return commands
        else:
            return None

    def resources_all_parameters_by_command(self, name: str, command: str) -> list:
        parameters = []
        for r in filter(lambda x: x['name'] == name, self.resources):
            try:
                for p in r['commands'][command]:
                    parameters.append(p['name'])
                return parameters
            except KeyError:
                return None

    def resources_all_required_parameters_by_command(self, name: str, command: str) -> list:
        parameters = []
        for r in filter(lambda x: x['name'] == name, self.resources):
            try:
                for p in r['commands'][command]:
                    if p['required']:
                        parameters.append(p['name'])
                return parameters
            except KeyError:
                return None

    def _validate_case(self, case: dict, path: str) -> bool:
        try:
            # Verify schema
            jsonschema.validate(case, self._schema)

            # Verify values
            for step in case['steps']:
                # Check if resource is known
                names = self.resources_all_names()
                if names is None or step['resource'] not in names:
                    if self._verbose:
                        print(f'{path}: Unknown resource "{step["resource"]}"',
                              file=sys.stderr)
                    return False

                # Check if command is known
                commands = self.resources_all_commands_by_name(step['resource'])
                if commands is None or step['command'] not in commands:
                    if self._verbose:
                        print(f'{path}: Unknown command "{step["command"]}" for'
                              f'resource "{step["resource"]}"', file=sys.stderr)
                    return False

                # Check if parameters are known
                parameters = self.resources_all_parameters_by_command(step['resource'], step['command'])
                if parameters is None:
                    return False

                for p in step['parameters'].keys():
                    if p not in parameters:
                        if self._verbose:
                            print(f'{path}: Unkown parameter "{p}" for command '
                                  f'"{step["command"]}" of resource '
                                  f'"{step["resource"]}', file=sys.stderr)
                        return False

                # Check if all required parameters are provided
                parameters = self.resources_all_required_parameters_by_command(step['resource'], step['command'])
                for p in parameters:
                    if p not in step['parameters'].keys():
                        if self._verbose:
                            print(f'{path}: Missing required parameter "{p}" for '
                                  f'command "{step["command"]}" of resource '
                                  f'"{step["resource"]}"', file=sys.stderr)
                        return False

        except jsonschema.ValidationError:
            if self._verbose:
                print(f'{path}: JSON schema violation', file=sys.stderr)
            return False

        return True

    def run(self, case: dict) -> bool:
        return True

    def list(self):
        cases = []

        for root, dirs, files in os.walk(self._main_directory):
            for file in files:
                if os.path.basename(file) == METADATA_FILE:
                    path = os.path.join(root, file)
                    with open(path, 'r') as f:
                        data = json.load(f)
                        if self._validate_case(data, path):
                            cases.append(data)

        return cases

    @property
    def main_directory(self):
        return self._main_directory
