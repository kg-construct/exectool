#!/usr/bin/env python3

import os
import sys
import json
import jsonschema
import importlib
import inspect
import shutil
from glob import glob
from datetime import datetime
from time import time, sleep
from typing import Tuple
from threading import Thread, Event
from statistics import mean, median

METADATA_FILE = 'metadata.json'
SCHEMA_FILE = 'metadata.schema'
DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')
WAIT_TIME = 5 # seconds
METRIC_TYPE_MEASUREMENT = 'MEASUREMENT'
METRIC_TYPE_INIT = 'INIT'
METRIC_TYPE_START = 'START'
METRIC_TYPE_STOP = 'STOP'

def _collect_metrics(stop_event: Event, active_resources: list,
                     interval: float):
    running_containers = []
    while not stop_event.wait(0):
        for r, f, name in active_resources:
            if not hasattr(r, 'stats') or f.closed:
                continue

            metrics = r.stats(silence_failure=True)
            if metrics is None:
                # Container was running and now not anymore, write stop metric
                # This codepath is only triggered when the container exits on
                # its own.
                if name in running_containers:
                    metrics = {
                                'type': METRIC_TYPE_STOP,
                                'time': time(),
                                'name': name,
                                'interval': interval
                              }
                    m = json.dumps(metrics).replace('\n', '') + '\n'
                    f.write(m)
                    f.flush()
                    f.close()
                continue

            if name not in running_containers:
                metrics_start = {
                                  'type': METRIC_TYPE_INIT,
                                  'time': time(),
                                  'name': name,
                                  'interval': interval
                                }
                m = json.dumps(metrics_start).replace('\n', '') + '\n'
                f.write(m)
                running_containers.append(name)

            # JSONL dump: replace all new lines and append a single new line
            # at the end and dump it to the file
            metrics['name'] = name
            metrics['type'] = METRIC_TYPE_MEASUREMENT
            metrics['interval'] = interval
            m = json.dumps(metrics).replace('\n', '') + '\n'
            f.write(m)
        sleep(interval)

    # Some containers keep running throughout the whole case, write the stop
    # metric once the metric measurement thread exists as the case is finished
    # then anyway.
    for r, f, name in active_resources:
        if not f.closed:
            metrics = {
                        'type': METRIC_TYPE_STOP,
                        'time': time(),
                        'name': name,
                        'interval': interval
                      }
            m = json.dumps(metrics).replace('\n', '') + '\n'
            f.write(m)
            f.flush()
            f.close()

class Executor:
    def __init__(self, main_directory: str, verbose: bool = False,
                 cli: bool = False):
        self._main_directory = main_directory
        self._schema = {}
        self._resources = None
        self._class_module_mapping = {}
        self._verbose = verbose
        self._cli = cli

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
            for name, cls in inspect.getmembers(imported_module, inspect.isclass):
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

                if name not in list(filter(lambda x: x['name'], self._resources)):
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

    def _resources_all_parameters_by_command(self, name: str, command: str) -> list:
        parameters = []
        for r in filter(lambda x: x['name'] == name, self._resources):
            try:
                for p in r['commands'][command]:
                    parameters.append(p['name'])
                return parameters
            except KeyError:
                return None

    def _resources_all_required_parameters_by_command(self, name: str, command: str) -> list:
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
                commands = self._resources_all_commands_by_name(step['resource'])
                if commands is None or step['command'] not in commands:
                    if self._verbose:
                        print(f'{path}: Unknown command "{step["command"]}" for'
                              f'resource "{step["resource"]}"', file=sys.stderr)
                    return False

                # Check if parameters are known
                parameters = self._resources_all_parameters_by_command(step['resource'], step['command'])
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
                parameters = self._resources_all_required_parameters_by_command(step['resource'], step['command'])
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

    def _print_step(self, resource: str, name: str, success: bool):
        if not self._cli:
            return

        if success:
            print(f'        ✅ {resource : <20}: {name : <50}')
        else:
            print(f'        ❌ {resource : <20}: {name : <50}')

    def _parse_metrics(self, results_path: str):
        mapping = {}
        for run_path in glob(f'{results_path}/run_*/'):
            try:
                run = os.path.split(os.path.dirname(run_path))[-1]
                run = run.replace('run_', '')
                run = int(run)
            except ValueError:
                print(f'Run "{run}" is not a number', file=sys.stderr)
                return False, {}

            # Get metrics for each run
            for resource_path in glob(f'{run_path}/*'):
                resource = os.path.split(resource_path)[-1]
                metrics_file = os.path.join(resource_path, 'metrics.jsonl')
                if not os.path.exists(metrics_file):
                    print(f'Metrics for resource "{resource}" in run {run} of '
                          f'case "{data["name"]}" does not exist',
                          file=sys.stderr)
                    return False, {}

                # Parse JSONL file
                metrics = []
                with open(metrics_file, 'r') as f:
                    start_time = None
                    for index, line in enumerate(f.readlines()):
                        m = json.loads(line)
                        m['index'] = index + 1
                        if m['type'] == METRIC_TYPE_INIT or \
                           (m['name'] == 'Query' and m['type'] == METRIC_TYPE_START):
                            start_time = m['time']
                        elif start_time is not None:
                            # In rare cases we may end up with -0.0 due to
                            # floating point errors, filter them out
                            m['diff'] = abs(round(m['time'] - start_time, 2))
                        metrics.append(m)

                # Store mapping to apply statistics on
                if resource not in mapping:
                    mapping[resource] = {}
                mapping[resource][str(run)] = metrics

        return True, mapping

    def _stats_execution_time(self, resource: dict, metrics: dict):
        diffs = []
        for run in metrics[resource].keys():
            execution_time = None
            for entry in metrics[resource][run]:
                if entry['type'] == METRIC_TYPE_STOP:
                    execution_time = entry['diff']
                    break

            if execution_time is None:
                print(f'Invalid metrics file in case "{data["name"]}"',
                      file=sys.stderr)
                return False

            diffs.append(execution_time)

        # We want a real datapoint as median value so only an uneven number of
        # runs are allowed!
        time_median = median(diffs)
        index = diffs.index(time_median) + 1

        return mean(diffs), time_median, index

    def _stats_find_run(self, resource: dict, metrics: dict,
                        time_median: float) -> dict:
        for run in metrics[resource].keys():
            execution_time = None
            for entry in metrics[resource][run]:
                if entry['type'] == METRIC_TYPE_STOP \
                    and entry['diff'] == time_median:
                        return metrics[resource][run]

    def stats(self, case: dict) -> bool:
        data = case['data']
        directory = case['directory']
        results_path = os.path.join(directory, 'results')

        if not os.path.exists(results_path):
            print(f'Results do not exist for case "{data["name"]}"',
                  file=sys.stderr)
            return False

        # Parse JSONL metric files by resource name
        success, metrics = self._parse_metrics(results_path)
        if not success:
            print(f'Cannot parse results for case "{data["name"]}"',
                  file=sys.stderr)
            return False

        for resource in metrics.keys():
            if len(metrics[resource].keys()) % 2 == 0:
                print(f'Number of runs must be uneven to generate statistics',
                      file=sys.stderr)
                return False

            # We guarantee that median is always calculated from uneven number
            # of runs, the median is always a datapoint. Take the run matching
            # with this median value as median and average run values
            diff_mean, diff_median, run = self._stats_execution_time(resource,
                                                                     metrics)
            #print(diff_mean, diff_median, metrics[resource][str(run)])
            aggregated_metrics = {}
            aggregated_metrics['resource'] = resource
            aggregated_metrics['measurements'] = metrics[resource][str(run)]
            aggregated_metrics['diff_mean'] = diff_mean
            aggregated_metrics['diff_median'] = diff_median

            aggregated_metrics_path = os.path.join(directory,
                                                   'results',
                                                   resource,
                                                   'metrics_aggregated.json')
            os.makedirs(os.path.dirname(aggregated_metrics_path), exist_ok=True)
            with open(aggregated_metrics_path, 'w') as f:
                json.dump(aggregated_metrics, f, indent=True)
        return True

    def clean(self, case: dict):
        # Checkpoints
        checkpoint_file = os.path.join(case['directory'], '.done')
        if os.path.exists(checkpoint_file):
            os.remove(checkpoint_file)

        # Results: log files, metric measurements
        for result_dir in glob(f'{case["directory"]}/results'):
            shutil.rmtree(result_dir)

    def run(self, case: dict, interval: float, run: int, checkpoint: bool) -> Tuple[bool, float]:
        success = True
        start = time()
        data = case['data']
        directory = case['directory']
        data_path = os.path.join(directory, 'data')
        results_path = os.path.join(directory, 'results')
        os.makedirs(data_path, exist_ok=True)
        checkpoint_file = os.path.join(directory, '.done')

        if os.path.exists(checkpoint_file):
            print(f'        ⏩ SKIPPED')
            return True, 0.0

        # Initialize resources if needed
        # Some resources have to perform an initialization step such as
        # configuring database users, storage, etc. which is only done once
        for step in data['steps']:
            module = self._class_module_mapping[step['resource']]
            resource = getattr(module, step['resource'])(data_path,
                                                         self._verbose)
            if hasattr(resource, 'initialization'):
                success = resource.initialization()
                self._print_step('Initializing', step['resource'], success)

        # Launch metrics thread
        stop_event = Event()
        active_resources = []
        metrics_thread = Thread(target=_collect_metrics,
                                args=(stop_event, active_resources, interval),
                                daemon=True)
        metrics_thread.start()

        # Execute steps
        for step in data['steps']:
            module = self._class_module_mapping[step['resource']]
            resource = getattr(module, step['resource'])(data_path,
                                                         self._verbose)
            root_mount_directory = resource.root_mount_directory()

            # Allow metrics thread to retrieve stats from container
            path = os.path.join(data_path, root_mount_directory)
            os.makedirs(path, exist_ok=True)
            path = os.path.join(path, 'metrics.jsonl')
            f = open(path, 'w')
            metrics = {
                        'type': METRIC_TYPE_START,
                        'time': time(),
                        'name': step['resource'],
                        'interval': interval
                      }
            m = json.dumps(metrics).replace('\n', '') + '\n'
            f.write(m)
            active_resources.append((resource, f, step['resource']))

            # Containers may need to start up first before executing a command
            if hasattr(resource, 'wait_until_ready'):
                if not resource.wait_until_ready():
                    success = False
                    self._print_step(step['resource'], step['name'], success)
                    break

            # Execute command
            command = getattr(resource, step['command'])
            if not command(**step['parameters']):
                success = False
                self._print_step(step['resource'], step['name'], success)
                break

            # Store logs
            # Needs separate process for logs and metrics collecting
            if hasattr(resource, 'logs'):
                path = os.path.join(data_path,
                                    root_mount_directory,
                                    'logs.txt')
                with open(path, 'w') as f:
                    f.writelines(resource.logs())

            # Step complete
            self._print_step(step['resource'], step['name'], success)

        diff = time() - start
        sleep(WAIT_TIME)

        # Stop all metric measurement threads
        stop_event.set()
        metrics_thread.join()

        # Stop active containers
        for r, f, name in active_resources:
            if r is not None and hasattr(r, 'stop'):
                r.stop()

        self._print_step('Cleaner', 'Clean up resources', success)

        # Mark checkpoint if necessary
        if checkpoint:
            with open(checkpoint_file, 'w') as f:
                f.write(f'{datetime.now().replace(microsecond=0).isoformat()}\n')

        # Move results for a clean slate when doing multiple runs
        results_run_path = os.path.join(results_path, f'run_{run}')
        os.makedirs(results_run_path, exist_ok=True)
        # Log files
        for log_file in glob(f'{data_path}/*/logs.txt'):
            subdir = log_file.replace(f'{data_path}/', '') \
                    .replace('/logs.txt', '')
            os.makedirs(os.path.join(results_run_path, subdir), exist_ok=True)
            shutil.move(log_file, os.path.join(results_run_path, subdir,
                                               'logs.txt'))

        # Metrics measurements
        for metrics_file in glob(f'{data_path}/*/metrics.jsonl'):
            subdir = metrics_file.replace(f'{data_path}/', '') \
                    .replace('/metrics.jsonl', '')
            os.makedirs(os.path.join(results_run_path, subdir), exist_ok=True)
            shutil.move(metrics_file, os.path.join(results_run_path, subdir,
                                               'metrics.jsonl'))

        return success, diff

    def list(self):
        cases = []

        for root, dirs, files in os.walk(self._main_directory):
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
