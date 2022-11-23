#!/usr/bin/env python3

import os
import sys
import csv
import json
import jsonschema
import importlib
import inspect
import shutil
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
from glob import glob
from datetime import datetime
from time import time, sleep
from typing import Tuple, Optional
from threading import Thread, Event
from queue import Queue, Empty
from statistics import mean, median

METADATA_FILE = 'metadata.json'
SCHEMA_FILE = 'metadata.schema'
DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')
CONFIG_DIR = os.path.join(os.path.dirname(__file__), 'config')
WAIT_TIME = 5 # seconds
METRIC_VERSION_1 = 1
METRIC_TYPE_MEASUREMENT = 'MEASUREMENT'
METRIC_TYPE_INIT = 'INIT'
METRIC_TYPE_START = 'START'
METRIC_TYPE_STOP = 'STOP'
METRIC_TYPE_EXIT = 'EXIT'
LOGS_FILE_NAME = 'logs.txt'
METRICS_FILE_NAME = 'metrics.jsonl'
PLOT_MARGIN = 1.1

# Thread-safe JSONL writer
class _JSONLWriter():
    def __init__(self, stop_event: Event, jsonl_file: str):
        self._file = open(jsonl_file, 'w')
        self._queue = Queue()
        self._stop_event = stop_event
        self._writing_thread = Thread(target=self._write_queue,
                                      args=(self._queue,), daemon=True)
        self._writing_thread.start()

    def _write_queue(self, q: Queue):
        while not self._stop_event.wait(0):
            try:
                row = q.get(timeout=1)
            except Empty:
                continue
            r = json.dumps(row, sort_keys=True).replace('\n', '') + '\n'
            self._file.write(r)
            q.task_done()

        self._file.close()

    def writerows(self, rows):
        for r in rows:
            self.writerow(r)

    def writerow(self, row):
        self._queue.put(row)

    def join(self):
        self._writing_thread.join()

def _collect_metrics(stop_event: Event, active_resources: list,
                     interval: float, metrics_writer: _JSONLWriter):
    init_done_containers = []
    exit_done_containers = []
    while not stop_event.wait(0):
        timestamp = time()
        for resource in active_resources:
            if not hasattr(resource, 'stats'):
                continue

            metrics = resource.stats(silence_failure=True)
            name = resource.name.replace('-', '')
            if metrics is None:
                # Container was started but is not running anymore, write stop
                # metric. This codepath is only triggered when the container
                # exits on its own.
                if resource.started and name not in exit_done_containers:
                    metrics_exit = {
                                     'version': METRIC_VERSION_1,
                                     'type': METRIC_TYPE_EXIT,
                                     'time': timestamp,
                                     'resource': name,
                                     'interval': interval
                                   }
                    metrics_writer.writerow(metrics_exit)
                    exit_done_containers.append(name)
                continue

            if name not in init_done_containers:
                metrics_init = {
                                 'version': METRIC_VERSION_1,
                                 'type': METRIC_TYPE_INIT,
                                 'time': timestamp,
                                 'resource': name,
                                 'interval': interval
                               }
                metrics_writer.writerow(metrics_init)
                init_done_containers.append(name)

            metrics['version'] = METRIC_VERSION_1
            metrics['resource'] = name
            metrics['type'] = METRIC_TYPE_MEASUREMENT
            metrics['interval'] = interval
            metrics['time'] = timestamp
            metrics_writer.writerow(metrics)

        # Wait the specific interval but substract the time we needed to
        # collect the metrics so we still have our fixed interval
        delta = time() - timestamp
        if (interval - delta > 0.0):
            sleep(interval - delta)

    # Some containers keep running throughout the whole case, write the stop
    # metric once the metric measurement thread exists as the case is finished
    # then anyway.
    for resource in active_resources:
        name = resource.name.replace('-', '')

        # Exited containers do not need to log multiple times their exit
        if name in exit_done_containers or name in ['Query']:
            continue

        metrics = {
                    'version': METRIC_VERSION_1,
                    'type': METRIC_TYPE_EXIT,
                    'time': timestamp,
                    'resource': name,
                    'interval': interval
                  }
        metrics_writer.writerow(metrics)

class Executor:
    def __init__(self, main_directory: str, verbose: bool = False,
                 cli: bool = False):
        self._main_directory = main_directory
        self._schema = {}
        self._resources = None
        self._class_module_mapping = {}
        self._verbose = verbose
        self._cli = cli
        self._colors = [ 'red', 'green', 'blue', 'orange', 'purple' ]

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

    def _print_step(self, resource: str, name: str, success: bool):
        if not self._cli:
            return

        if success:
            print(f'        ✅ {resource : <20}: {name : <50}')
        else:
            print(f'        ❌ {resource : <20}: {name : <50}')

    def _parse_metrics(self, results_path: str) -> Optional[dict]:
        metrics = {}
        for run_path in glob(f'{results_path}/run_*/'):
            try:
                run = os.path.split(os.path.dirname(run_path))[-1]
                run = run.replace('run_', '')
                run = int(run)
            except ValueError:
                print(f'Run "{run}" is not a number', file=sys.stderr)
                return None

            metrics_file = os.path.join(run_path, METRICS_FILE_NAME)
            if not os.path.exists(metrics_file):
                print(f'Metrics file "{metrics_file}" does not exist',
                      file=sys.stderr)
                return None

            relative_time = None
            execution_time = {}
            with open(metrics_file, 'r') as f:
                metrics[f'run_{run}'] = []
                for index, line in enumerate(f.readlines()):
                    try:
                        m = json.loads(line)
                    except json.JSONDecodeError:
                        print(f'Cannot parse as JSON: "{line}"')
                        return None
                    resource = m['resource'].replace('-', '') # FIX for unescaped metrics
                    m['index'] = index + 1

                    # Relative timestamp for the whole case
                    if m['type'] == METRIC_TYPE_START and relative_time is None:
                        relative_time = m['time']
                        m['relative_time'] = 0.0
                    else:
                        t = abs(round(m['time'] - relative_time, 2))
                        m['relative_time'] = t

                    # Execution time of each resource, the container
                    # setup and shutdown times are removed, same for the
                    # benchmark overhead of stopping and starting containers
                    if m['type'] == METRIC_TYPE_INIT \
                       and resource not in execution_time:
                        execution_time[resource] = m['time']
                        m['execution_time'] = 0.0
                    elif m['type'] == METRIC_TYPE_EXIT \
                         and resource in execution_time:
                        t = abs(round(m['time'] - execution_time[resource], 2))
                        m['execution_time'] = t
                        del execution_time[resource]
                    elif m['type'] == METRIC_TYPE_MEASUREMENT \
                         and resource in execution_time:
                        t = abs(round(m['time'] - execution_time[resource], 2))
                        m['execution_time'] = t

                    metrics[f'run_{run}'].append(m)

        return metrics

    def _aggregate_runs(self, metrics: dict) -> dict:
        # Each key of metrics dict is a run
        execution_times = {}
        steps_by_run = {}
        for run in sorted(metrics.keys()):
            # Store the execution time of each step for each run
            step_number = 1
            start_time = None
            steps_by_run[run] = {}
            for entry in metrics[run]:
                step_name = f'step{step_number}'
                if step_name not in steps_by_run[run]:
                    steps_by_run[run][step_name] = []
                steps_by_run[run][step_name].append(entry)
                # Step started
                if entry['type'] == METRIC_TYPE_START:
                    start_time = entry['time']
                # Remove Docker initialization time if applicable
                elif entry['type'] == METRIC_TYPE_INIT:
                    start_time = entry['time']
                # Step ended
                elif entry['type'] == METRIC_TYPE_STOP:
                    diff = entry['time'] - start_time
                    if step_name not in execution_times:
                        execution_times[step_name] = []
                    execution_times[step_name].append(diff)
                    # Prepare for next step
                    start_time = None
                    step_number += 1

        # Find the median step over all runs and calculate some stats on it
        metrics['stats'] = {}
        for step, values in execution_times.items():
            try:
                median_run_for_step = values.index(median(values)) + 1
            except ValueError:
                print(values, median(values))
            measurements_for_step = steps_by_run[f'run_{median_run_for_step}'][step]

            metrics['stats'][step] = {
                'execution_time_mean': mean(values),
                'execution_time_median': median(values),
                'execution_time_values': values,
                'median_run_for_step': median_run_for_step,
                'measurements': measurements_for_step
            }

        # Build a median run out of all median steps
        median_run = []
        for key, value in metrics['stats'].items():
            median_run += value['measurements']
        metrics['stats']['median_run'] = median_run

        return metrics

    def _generate_plots(self, metrics: dict, directory: str):
        fig_path = os.path.join(directory, 'results', 'graphs.png')
        m = metrics['stats']['median_run']

        # Re-use the execution time X-axis, 4 metrics: CPU, memory, IO, network
        fig, ax = plt.subplots(4, 1, figsize=(12, 8))
        ax[len(ax) - 1].set_xlabel(f'Case execution time (s)')
        resources = sorted([*set(x['resource'].replace('-', '') for x in m)])

        for index, metric in enumerate(['cpu_total_time', 'memory_total_size']):
            y_ticks = []
            for color_index, r in enumerate(resources):
                x = []
                y = []
                color = self._colors[color_index % len(self._colors)]
                for entry in filter(lambda y: y['resource'].replace('-', '') == r, m):
                    if entry['type'] == METRIC_TYPE_INIT:
                        x.append(entry['relative_time'])
                        y.append(0.0)
                    elif entry['type'] == METRIC_TYPE_MEASUREMENT:
                        x.append(entry['relative_time'])

                        if metric == 'memory_total_size':
                            y.append(entry[metric] / 10**3)
                        elif metric == 'cpu_total_time':
                            y.append(entry[metric])
                        else:
                            raise ValueError(f'Cannot plot metric: "{metric}"')
                    elif entry['type'] == METRIC_TYPE_START:
                        ax[index].axvline(entry['relative_time'], color='grey',
                                          linestyle='dotted')

                ax[index].plot(x, y, color=color)
                if y:
                    y_ticks.append(min(y))
                    y_ticks.append(max(y))

                # Subplot titles
                if metric == 'memory_total_size':
                    ax[index].set_title('Total memory size vs execution time')
                    ax[index].set_ylabel(f'Total memory size (MB)')
                elif metric == 'cpu_total_time':
                    ax[index].set_title('Total CPU time vs execution time')
                    ax[index].set_ylabel(f'Total CPU time (s)')
                else:
                    raise ValueError(f'Cannot plot metric: "{metric}"')

            if y_ticks:
                ax[index].set_ylim(0.0, max(y_ticks) * PLOT_MARGIN)

        for index, metric in enumerate(['io', 'network']):
            y_ticks = []
            for color_index, r in enumerate(resources):
                x = []
                y1 = {}
                y2 = {}
                color = self._colors[color_index % len(self._colors)]
                for entry in filter(lambda y: y['resource'].replace('-', '') == r, m):
                    if entry['type'] == METRIC_TYPE_START:
                        ax[index + 2].axvline(entry['relative_time'],
                                              color='grey', linestyle='dotted')

                    if metric not in entry:
                        continue

                    if entry['type'] == METRIC_TYPE_INIT \
                       or entry['type'] == METRIC_TYPE_MEASUREMENT:
                            x.append(entry['relative_time'])

                    for data in entry[metric]:
                        device = data['device']
                        if device not in y1:
                            y1[device] = []

                        if device not in y2:
                            y2[device] = []

                        if entry['type'] == METRIC_TYPE_INIT:
                            y1[device].append(0.0)
                            y2[device].append(0.0)
                        elif entry['type'] == METRIC_TYPE_MEASUREMENT:
                            if metric == 'io':
                                value_read = data['total_size_read'] / 10**3
                                value_write = data['total_size_write'] / 10**3
                                y1[device].append(value_read)
                                y2[device].append(value_write)
                                y_ticks.append(value_read)
                                y_ticks.append(value_write)
                            elif metric == 'network':
                                value_received = data['total_size_received'] / 10**3
                                value_transmitted = data['total_size_transmitted'] / 10**3
                                y1[device].append(value_received)
                                y2[device].append(value_transmitted)
                                y_ticks.append(value_received)
                                y_ticks.append(value_transmitted)
                            else:
                                raise ValueError(f'Cannot plot metric: "{metric}"')

                # Receive/read
                for device in y1.keys():
                    ax[index + 2].plot(x, y1[device], color=color)

                # Transmit/write
                for device in y2.keys():
                    ax[index + 2].plot(x, y2[device], color=color,
                                       linestyle='dashed')

                # Subplot titles
                if metric == 'io':
                    ax[index + 2].set_title('IO read/write vs execution time')
                    ax[index + 2].set_ylabel(f'Data size (MB)')
                elif metric == 'network':
                    ax[index + 2].set_title('Network receive/transmit vs execution time')
                    ax[index + 2].set_ylabel(f'Data size (MB)')
                else:
                    raise ValueError(f'Cannot plot metric: "{metric}"')

            ax[index + 2].set_ylim(0.0, max(y_ticks) * PLOT_MARGIN)

        handles = []
        for color_index, r in enumerate(resources):
            color = self._colors[color_index % len(self._colors)]
            label = mpatches.Patch(color=color, label=r)
            handles.append(label)
        fig.tight_layout()
        fig.legend(handles=handles, loc='lower center',
                   bbox_to_anchor=(0.5, -0.05),
                   fancybox=False, shadow=False,
                   ncol=len(handles))

        plt.savefig(fig_path, bbox_inches='tight')
        plt.close(fig)

    def stats(self, case: dict) -> bool:
        data = case['data']
        directory = case['directory']
        results_path = os.path.join(directory, 'results')

        if not os.path.exists(results_path):
            print(f'Results do not exist for case "{data["name"]}"',
                  file=sys.stderr)
            return False

        # Parse JSONL metric file for each run
        metrics = self._parse_metrics(results_path)
        if metrics is None:
            print(f'Cannot parse results for case "{data["name"]}"',
                  file=sys.stderr)
            return False

        # We guarantee that median is always calculated from uneven number
        # of runs, the median is always a datapoint. Take the run matching
        # with this median value as median and average run values
        if len(metrics.keys()) % 2 == 0:
            print(f'Number of runs must be uneven to generate statistics',
                  file=sys.stderr)
            return False

        aggregated_metrics = self._aggregate_runs(metrics)
        aggregated_metrics_path = os.path.join(directory, 'results',
                                               'aggregated.json')
        with open(aggregated_metrics_path, 'w') as f:
            json.dump(aggregated_metrics, f, indent=True)

        # Generate plots of CPU, memory, IO, network against execution time
        self._generate_plots(metrics, directory)

        return True

    def clean(self, case: dict):
        # Checkpoints
        checkpoint_file = os.path.join(case['directory'], '.done')
        if os.path.exists(checkpoint_file):
            os.remove(checkpoint_file)

        # Results: log files, metric measurements
        for result_dir in glob(f'{case["directory"]}/results'):
            shutil.rmtree(result_dir)

        # Data: persistent storage
        for data_dir in glob(f'{case["directory"]}/data/*'):
            if not data_dir.endswith('shared'):
                shutil.rmtree(data_dir)

    def run(self, case: dict, interval: float, run: int,
            wait_for_user: bool, checkpoint: bool) -> Tuple[bool, float]:
        success = True
        start = time()
        data = case['data']
        directory = case['directory']
        data_path = os.path.join(directory, 'data')
        results_run_path = os.path.join(directory, 'results', f'run_{run}')
        checkpoint_file = os.path.join(directory, '.done')

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
                self._print_step('Initializing', step['resource'], success)

        # Launch metrics thread
        stop_event = Event()
        active_resources = []
        metrics_file = os.path.join(results_run_path, METRICS_FILE_NAME)
        metrics_writer = _JSONLWriter(stop_event, metrics_file)
        metrics_thread = Thread(target=_collect_metrics,
                                args=(stop_event, active_resources,
                                      interval, metrics_writer),
                                daemon=True)
        metrics_thread.start()

        # Execute steps
        for step in data['steps']:
            success = True
            module = self._class_module_mapping[step['resource']]
            resource = getattr(module, step['resource'])(data_path, CONFIG_DIR,
                                                         self._verbose)

            # Allow metrics thread to retrieve stats from container
            path = os.path.join(data_path, resource.root_mount_directory)
            os.makedirs(path, exist_ok=True)
            metrics = {
                        'version': METRIC_VERSION_1,
                        'type': METRIC_TYPE_START,
                        'time': time(),
                        'resource': step['resource'],
                        'interval': interval,
                        '@id': step['@id']
                      }
            metrics_writer.writerow(metrics)
            active_resources.append(resource)
            start_step = time()

            # non-container resources do not have INIT, add manually
            # TODO: figure out a better way for non-container resources
            if step['resource'] in ['Query']:
                metrics_init = {
                                 'version': METRIC_VERSION_1,
                                 'type': METRIC_TYPE_INIT,
                                 'time': time(),
                                 'resource': step['resource'],
                                 'interval': interval
                               }
                metrics_writer.writerow(metrics_init)

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
                # TODO: queries may fail, but are not critical, still continue
                if step['resource'] not in ['Query']:
                    self._print_step(step['resource'], step['name'], success)
                    break

            # non-container resources do not have EXIT, add manually
            # TODO: figure out a better way for non-container resources
            if step['resource'] in ['Query']:
                    metrics_exit = {
                                     'version': METRIC_VERSION_1,
                                     'type': METRIC_TYPE_EXIT,
                                     'time': time(),
                                     'resource': step['resource'],
                                     'interval': interval
                                   }
                    metrics_writer.writerow(metrics_exit)

            # Store execution time of each step
            diff_step = abs(round(time() - start_step, 2))
            metrics = {
                        'version': METRIC_VERSION_1,
                        'type': METRIC_TYPE_STOP,
                        'time': time(),
                        'resource': step['resource'],
                        'interval': interval,
                        '@id': step['@id'],
                      }
            metrics_writer.writerow(metrics)

            # Store logs
            # Needs separate process for logs and metrics collecting
            if hasattr(resource, 'logs'):
                path = os.path.join(data_path,
                                    resource.root_mount_directory,
                                    LOGS_FILE_NAME)
                with open(path, 'w') as f:
                    f.writelines(resource.logs())

            # Step complete
            self._print_step(step['resource'], step['name'], success)
            if wait_for_user:
                input('Step completed, press any key to continue...')

        # Case finished, store diff time
        diff = time() - start

        # Stop all metric measurement and writing threads
        stop_event.set()
        metrics_thread.join()
        metrics_writer.join()

        # Stop active containers
        for resource in active_resources:
            if resource is not None and hasattr(resource, 'stop'):
                resource.stop()

        self._print_step('Cleaner', 'Clean up resources', True)

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
                        print('Cannot find file: {p1}', file=sys.stderr)

                if step['parameters'].get('output_file', False) \
                        and not step['parameters'].get('multiple_files', False):
                    output_file = step['parameters']['output_file']
                    p1 = os.path.join(directory, 'data/shared', output_file)
                    p2 = os.path.join(results_run_path, subdir, output_file)
                    try:
                        shutil.move(p1, p2)
                    except FileNotFoundError:
                        print('Cannot find file: {p1}', file=sys.stderr)

        self._print_step('Cooldown', f'Hardware cooldown period {WAIT_TIME}s',
                         success)
        sleep(WAIT_TIME)

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
