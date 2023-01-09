#!/usr/bin/env python3

import os
import sys
import csv
import json
import jsonschema
import importlib
import inspect
import shutil
from glob import glob
from datetime import datetime
from time import time, sleep
from typing import Tuple, Optional
from threading import Thread, Event
from queue import Queue, Empty
try:
    from bench_executor import Collector, METRICS_FILE_NAME, Stats
except ModuleNotFoundError:
    from collector import Collector, METRICS_FILE_NAME
    from stats import Stats


METADATA_FILE = 'metadata.json'
SCHEMA_FILE = 'metadata.schema'
DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')
CONFIG_DIR = os.path.join(os.path.dirname(__file__), 'config')
WAIT_TIME = 15 # seconds
LOGS_FILE_NAME = 'logs.txt'
PLOT_MARGIN = 1.1

# Dummy callback in case no callback was provided
def _progress_cb(resource: str, name: str, success: bool):
    pass

class Executor:
    def __init__(self, main_directory: str, verbose: bool = False,
                 progress_cb = _progress_cb):
        self._main_directory = main_directory
        self._schema = {}
        self._resources = None
        self._class_module_mapping = {}
        self._verbose = verbose
        self._progress_cb = progress_cb

        self._init_resources()

        with open(os.path.join(DATA_DIR, SCHEMA_FILE)) as f:
            self._schema = json.load(f)

    def _init_resources(self) -> list:
        if self._resources is not None:
            return self._resources
        else:
            self._resources = []

        # Discover all modules to import
        sys.path.append(os.path.dirname(__file__))
        self._modules = list(filter(lambda x: x.endswith('.py') \
                                    and '__init__' not in x \
                                    and '__pycache__' not in x,
                                    os.listdir(os.path.dirname(__file__))))

        # Discover all classes in each module
        for m in self._modules:
            module_name = os.path.splitext(m)[0]
            imported_module = importlib.import_module(module_name)
            for name, cls in inspect.getmembers(imported_module,
                                                inspect.isclass):
                if name.startswith('_') or name[0].islower():
                    continue

                # Store class-module mapping for reverse look-up
                self._class_module_mapping[name] = imported_module

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

                if name not in list(filter(lambda x: x['name'],
                                           self._resources)):
                    self._resources.append({'name': name, 'commands': methods})

    def _resources_all_names(self) -> list:
        names = []
        for r in self._resources:
            names.append(r['name'])

        if names:
            return names
        else:
            return None

    def _resources_all_commands_by_name(self, name: str) -> list:
        commands = []
        for r in filter(lambda x: x['name'] == name, self._resources):
            commands += list(r['commands'].keys())
        if commands:
            return commands
        else:
            return None

    def _resources_all_parameters_by_command(self, name: str,
                                             command: str) -> list:
        parameters = []
        for r in filter(lambda x: x['name'] == name, self._resources):
            try:
                for p in r['commands'][command]:
                    parameters.append(p['name'])
                return parameters
            except KeyError:
                return None

    def _resources_all_required_parameters_by_command(self, name: str,
                                                      command: str) -> list:
        parameters = []
        for r in filter(lambda x: x['name'] == name, self._resources):
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
                names = self._resources_all_names()
                if names is None or step['resource'] not in names:
                    if self._verbose:
                        print(f'{path}: Unknown resource "{step["resource"]}"',
                              file=sys.stderr)
                    return False

                # Check if command is known
                r = step['resource']
                commands = self._resources_all_commands_by_name(r)
                if commands is None or step['command'] not in commands:
                    if self._verbose:
                        print(f'{path}: Unknown command "{step["command"]}" for'
                              f'resource "{step["resource"]}"', file=sys.stderr)
                    return False

                # Check if parameters are known
                r = step['resource']
                c = step['command']
                parameters = self._resources_all_parameters_by_command(r, c)
                if parameters is None:
                    return False

                for p in step['parameters'].keys():
                    if p not in parameters:
                        if self._verbose:
                            print(f'{path}: Unkown parameter "{p}" for command '
                                  f'"{step["command"]}" of resource '
                                  f'"{step["resource"]}"', file=sys.stderr)
                        return False

                # Check if all required parameters are provided
                r = step['resource']
                c = step['command']
                parameters = \
                    self._resources_all_required_parameters_by_command(r, c)
                for p in parameters:
                    if p not in step['parameters'].keys():
                        if self._verbose:
                            print(f'{path}: Missing required parameter "{p}" '
                                  f'for command "{step["command"]}" '
                                  f'of resource "{step["resource"]}"',
                                  file=sys.stderr)
                        return False

        except jsonschema.ValidationError:
            if self._verbose:
                print(f'{path}: JSON schema violation', file=sys.stderr)
            return False

        return True

    def stats(self, case: dict) -> bool:
        data = case['data']
        directory = case['directory']
        results_path = os.path.join(directory, 'results')

        if not os.path.exists(results_path):
            print(f'Results do not exist for case "{data["name"]}"',
                  file=sys.stderr)
            return False

        stats = Stats(results_path, len(data['steps']))
        stats.aggregate()

        return True

    def clean(self, case: dict):
        # Checkpoints
        checkpoint_file = os.path.join(case['directory'], '.done')
        if os.path.exists(checkpoint_file):
            os.remove(checkpoint_file)

        # Results: log files, metric measurements, run checkpoints
        for result_dir in glob(f'{case["directory"]}/results'):
            shutil.rmtree(result_dir)

        # Data: persistent storage
        for data_dir in glob(f'{case["directory"]}/data/*'):
            if not data_dir.endswith('shared'):
                shutil.rmtree(data_dir)

    def run(self, case: dict, interval: float,
            run: int, checkpoint: bool) -> Tuple[bool, float]:
        success = True
        start = time()
        data = case['data']
        directory = case['directory']
        data_path = os.path.join(directory, 'data')
        results_run_path = os.path.join(directory, 'results', f'run_{run}')
        checkpoint_file = os.path.join(directory, '.done')
        run_checkpoint_file = os.path.join(results_run_path, '.done')
        active_resources = []

        # Make sure we start with a clean setup before the first run
        if run == 1:
            self.clean(case)

        # create directories
        os.umask(0)
        os.makedirs(data_path, exist_ok=True)
        os.makedirs(results_run_path, exist_ok=True)

        # Initialize resources if needed
        # Some resources have to perform an initialization step such as
        # configuring database users, storage, etc. which is only done once
        for step in data['steps']:
            module = self._class_module_mapping[step['resource']]
            resource = getattr(module, step['resource'])(data_path, CONFIG_DIR,
                                                         self._verbose)
            if hasattr(resource, 'initialization'):
                success = resource.initialization()
                self._progress_cb('Initializing', step['resource'], success)

        # Launch metrics collection
        collector = Collector(results_run_path, directory, interval,
                              len(data['steps']), run)

        # Execute steps
        for index, step in enumerate(data['steps']):
            success = True
            module = self._class_module_mapping[step['resource']]
            resource = getattr(module, step['resource'])(data_path, CONFIG_DIR,
                                                         self._verbose)
            active_resources.append(resource)

            # Containers may need to start up first before executing a command
            if hasattr(resource, 'wait_until_ready'):
                if not resource.wait_until_ready():
                    success = False
                    self._progress_cb(step['resource'], step['name'], success)
                    break

            # Execute command
            command = getattr(resource, step['command'])
            if not command(**step['parameters']):
                success = False
                # Some steps are non-critical like queries, they may fail but
                # should not cause a complete case failure. Allow these
                # failures if the may_fail key is present
                if step.get('may_fail', False):
                    self._progress_cb(step['resource'], step['name'], success)
                    continue
                else:
                    self._progress_cb(step['resource'], step['name'], success)
                    break

            # Store logs
            # Needs separate process for logs and metrics collecting
            if hasattr(resource, 'logs'):
                path = os.path.join(data_path,
                                    resource.root_mount_directory,
                                    LOGS_FILE_NAME)
                with open(path, 'w') as f:
                    f.writelines(resource.logs())

            # Step complete
            self._progress_cb(step['resource'], step['name'], success)

            # Step finished, let metric collector know
            if (index + 1) < len(data['steps']):
                collector.next_step()

        # Case finished, store diff time
        diff = time() - start

        # Stop metrics collection
        collector.stop()

        # Stop active containers
        for resource in active_resources:
            if resource is not None and hasattr(resource, 'stop'):
                resource.stop()

        self._progress_cb('Cleaner', 'Clean up resources', True)

        # Mark checkpoint if necessary
        if checkpoint and success:
            with open(checkpoint_file, 'w') as f:
                d = datetime.now().replace(microsecond=0).isoformat()
                f.write(f'{d}\n')

        # Move results for a clean slate when doing multiple runs
        # Log files
        for log_file in glob(f'{data_path}/*/{LOGS_FILE_NAME}'):
            subdir = log_file.replace(f'{data_path}/', '') \
                    .replace(f'/{LOGS_FILE_NAME}', '')
            os.makedirs(os.path.join(results_run_path, subdir), exist_ok=True)
            shutil.move(log_file, os.path.join(results_run_path, subdir,
                                               LOGS_FILE_NAME))

        # Metrics measurements
        for metrics_file in glob(f'{data_path}/*/{METRICS_FILE_NAME}'):
            subdir = metrics_file.replace(f'{data_path}/', '') \
                    .replace(f'/METRICS_FILE_NAME', '')
            os.makedirs(os.path.join(results_run_path, subdir), exist_ok=True)
            shutil.move(metrics_file, os.path.join(results_run_path, subdir,
                                                   METRICS_FILE_NAME))

        # Results: all 'output_file' and 'result_file' values
        if success:
            for step in data['steps']:
                subdir = step['resource'].lower().replace('_', '')
                if step['parameters'].get('results_file', False):
                    results_file = step['parameters']['results_file']
                    p1 = os.path.join(directory, 'data/shared', results_file)
                    p2 = os.path.join(results_run_path, subdir, results_file)
                    try:
                        shutil.move(p1, p2)
                    except FileNotFoundError:
                        print(f'Cannot find results file: {p1}',
                              file=sys.stderr)

                if step['parameters'].get('output_file', False) \
                        and not step['parameters'].get('multiple_files', False):
                    output_file = step['parameters']['output_file']
                    p1 = os.path.join(directory, 'data/shared', output_file)
                    p2 = os.path.join(results_run_path, subdir, output_file)
                    try:
                        shutil.move(p1, p2)
                    except FileNotFoundError:
                        print(f'Cannot find output file: {p1}', file=sys.stderr)

            # Run complete, mark it
            run_checkpoint_file = os.path.join(results_run_path, '.done')
            with open(run_checkpoint_file, 'w') as f:
                d = datetime.now().replace(microsecond=0).isoformat()
                f.write(f'{d}\n')

        self._progress_cb('Cooldown', f'Hardware cooldown period {WAIT_TIME}s',
                          True)
        sleep(WAIT_TIME)

        return success, diff

    def list(self):
        cases = []

        for directory in glob(self._main_directory):
            for root, dirs, files in os.walk(directory):
                for file in files:
                    if os.path.basename(file) == METADATA_FILE:
                        path = os.path.join(root, file)
                        with open(path, 'r') as f:
                            data = json.load(f)
                            if self._validate_case(data, path):
                                cases.append({'directory': os.path.dirname(path),
                                              'data': data})

        return cases

    @property
    def main_directory(self):
        return self._main_directory
