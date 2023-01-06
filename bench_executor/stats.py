#!/usr/bin/env python3

import os
import sys
from glob import glob
from statistics import median
from csv import DictWriter, DictReader
from collector import FIELDNAMES, METRICS_FILE_NAME

METRICS_AGGREGATED_FILE_NAME = 'aggregated.csv'
METRICS_SUMMARY_FILE_NAME = 'summary.csv'
FIELDNAMES_FLOAT = ['timestamp', 'cpu_user', 'cpu_system', 'cpu_idle',
                    'cpu_iowait', 'cpu_user_system']
FIELDNAMES_INT = ['index', 'step', 'version', 'memory_ram', 'memory_swap',
                  'memory_ram_swap', 'disk_read_count', 'disk_write_count',
                  'disk_read_bytes', 'disk_write_bytes', 'disk_read_time',
                  'disk_write_time', 'disk_busy_time', 'network_received_count',
                  'network_sent_count', 'network_received_bytes',
                  'network_sent_bytes', 'network_received_error',
                  'network_sent_error', 'network_received_drop',
                  'network_sent_drop']
FIELDNAMES_SUMMARY = [
    'number_of_samples',
    'step',
    'duration',
    'version',
    'cpu_user_diff',
    'cpu_system_diff',
    'cpu_user_system_diff',
    'cpu_idle_diff',
    'cpu_iowait_diff',
    'memory_ram_max',
    'memory_swap_max',
    'memory_ram_swap_max',
    'memory_ram_min',
    'memory_swap_min',
    'memory_ram_swap_min',
    'disk_read_count_diff',
    'disk_write_count_diff',
    'disk_read_bytes_diff',
    'disk_write_bytes_diff',
    'disk_read_time_diff',
    'disk_write_time_diff',
    'disk_busy_time_diff',
    'network_received_count_diff',
    'network_sent_count_diff',
    'network_received_bytes_diff',
    'network_sent_bytes_diff',
    'network_received_error_diff',
    'network_sent_error_diff',
    'network_received_drop_diff',
    'network_sent_drop_diff'
]
ROUND = 4

#
# Generate stats from the result runs by aggregating it on
# median execution time for each step. Processing is done for each step per
# run and unnecessary values are skipped to reduce the memory consumption.
#
# The median run is available in 'aggregated.csv' while a summarized version
# which only reports the diff or max value of each step in 'summary.csv'
#

class Stats():
    def __init__(self, results_path: str, number_of_steps: int):
        self._results_path = os.path.abspath(results_path)
        self._number_of_steps = number_of_steps

        if not os.path.exists(results_path):
            msg = f'Results do not exist: {results_path}'
            print(msg, file=sys.stderr)
            raise ValueError(msg)

    def _parse_field(self, field, value):
        if field in FIELDNAMES_FLOAT:
            return float(value)
        elif field in FIELDNAMES_INT:
            return int(value)
        else:
            msg = f'Field "{field}" type is unknown'
            print(msg, file=sys.stderr)
            raise ValueError(msg)

    def _parse(self, run_path, fields=FIELDNAMES, step=None):
        data = []

        metrics_file = os.path.join(run_path, METRICS_FILE_NAME)
        if not os.path.exists(metrics_file):
            print(f'Metrics file "{metrics_file}" does not exist',
                  file=sys.stderr)
            return []

        # Filter the fields we want from above, this way we don't load all
        # the data in memory during processing.
        with open(metrics_file, 'r') as f:
            reader = DictReader(f)
            for line in reader:
                # Skip steps we don't want to parse
                if step is not None and \
                   step != self._parse_field('step', line['step']):
                    continue

                # Filter on field names
                filtered = {key: line[key] for key in fields}
                entry = {}
                for key, value in filtered.items():
                    entry[key] = self._parse_field(key, value)

                data.append(entry)

        return data

    def aggregate(self) -> bool:
        # Find each median step of all runs before extracting more data for
        # memory consumption reasons
        runs = []
        for run_path in glob(f'{self._results_path}/run_*/'):
            # Extract run number
            try:
                run_id = os.path.split(os.path.dirname(run_path))[-1]
                run_id = run_id.replace('run_', '')
                run_id = int(run_id)
            except ValueError:
                print(f'Run "{run_id}" is not a number', file=sys.stderr)
                return False

            # Extract steps and timestamps of this run
            data = self._parse(run_path, fields=['step', 'timestamp'])

            # Calculate timestamp diff for each step
            step = 1
            timestamps = []
            step_end = 0.0
            step_begin = 0.0
            for entry in data:
                entry_step = entry['step']
                assert (entry_step >= step), 'Entry step decreased over time'

                # Next step
                if entry_step > step:
                    # Calculate diff of current step if at least 2 entries
                    # for the step exist, if not the diff is 0.0 and we fall
                    # back to the step_begin timestamp which will make sure we
                    # use the run with the timestamp that is the median of all
                    # runs. For example: [4.5, 5.0, 6.5] will return run 2 as
                    # 5.0 is the median.
                    diff = step_end - step_begin
                    if diff == 0.0:
                        print(f'Only 1 entry for step {step} found, falling '
                              'back to median timestamp instead of diff',
                              file=sys.stderr)
                        diff = step_begin

                    timestamps.append(diff)

                    # Reset for next step
                    step = entry_step
                    step_begin = entry['timestamp']
                    step_end = entry['timestamp']
                # step_end keeps increasing until the step changes
                else:
                    step_end = entry['timestamp']
            # Final step does not cause an increment, add manually
            timestamps.append(step_end - step_begin)
            runs.append((run_id, timestamps))

        # Statistics rely on uneven number of runs
        assert (len(runs) % 2 != 0), 'Number of runs should never be even'

        # Runs are unsorted as glob does not have a fixed order, sort them
        # based on run number in tuple
        runs.sort(key=lambda element: element[0])

        # Find median for each step across runs
        timestamps_by_step = []
        for step_index in range(self._number_of_steps):
            timestamps_by_step.append([])

        for run in runs:
            run_id = run[0]
            timestamps = run[1]

            # Do not process incomplete runs
            msg = f'Number of steps ({self._number_of_steps}) does not ' + \
                  f'match with extracted steps of run ({len(timestamps)}). ' + \
                  f'Skipping run {run_id}'
            assert (len(timestamps) == self._number_of_steps), msg

            # Create list of timestamps for each step from all runs
            for step_index in range(self._number_of_steps):
                timestamps_by_step[step_index].append(timestamps[step_index])

        # Create a list of our steps with the run_id which has the median value
        # for that step
        aggregated_entries = []
        summary_entries = []
        step_end_timestamp = 0.0
        for step_index, step_timestamps in enumerate(timestamps_by_step):
            # We ensure that the number of runs is always uneven so the median
            # is always a measured data point instead of the average of 2 data
            # points with even number of runs
            median_run_id = timestamps_by_step[step_index] \
                            .index(median(step_timestamps)) + 1
            median_run_path = os.path.join(self._results_path,
                                           f'run_{median_run_id}')
            median_step_data = self._parse(median_run_path, step=step_index + 1)
            aggregated_entries += median_step_data

            # Summary data of a step: diff per step
            summary = {}
            for field in FIELDNAMES:
                # Report max memory peak for this step
                if 'memory' in field:
                    values = []
                    for data in median_step_data:
                        values.append(data[field])
                    summary[f'{field}_min'] = min(values)
                    summary[f'{field}_max'] = max(values)
                # Leave some fields like they are
                elif field in ['version', 'step']:
                    summary[field] = median_step_data[0][field]
                # All other fields are accumulated data values for which we
                # report the diff for the step
                else:
                    first = median_step_data[0][field]
                    last = median_step_data[-1][field]
                    diff = round(last - first, ROUND)
                    if field == 'index':
                        # diff will be 0 for 1 sample, but we have this sample,
                        # so include it
                        summary['number_of_samples'] = diff + 1
                    elif field == 'timestamp':
                        summary['duration'] = diff
                    else:
                        summary[f'{field}_diff'] = diff
            summary_entries.append(summary)

        aggregated_file = os.path.join(self._results_path,
                                       METRICS_AGGREGATED_FILE_NAME)
        summary_file = os.path.join(self._results_path,
                                    METRICS_SUMMARY_FILE_NAME)

        # Store aggregated data
        with open(aggregated_file, 'w') as f:
            writer = DictWriter(f, fieldnames=FIELDNAMES)
            writer.writeheader()
            for entry in aggregated_entries:
                writer.writerow(entry)

        # Store summary data
        with open(summary_file, 'w') as f:
            writer = DictWriter(f, fieldnames=FIELDNAMES_SUMMARY)
            writer.writeheader()
            for entry in summary_entries:
                writer.writerow(entry)
