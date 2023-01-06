#!/usr/bin/env python3

import os
import sys
import platform
import shutil
import psutil
from docker import DockerClient
from csv import DictWriter
from time import time, sleep
from datetime import datetime
from subprocess import run, CalledProcessError
from threading import Thread, Event

#
# Hardware and case information is logged to 'case-info.txt' on construction.
#
# All data are stored in a CSV as 'stats.csv'.
# These data are accumulated among all CPU cores, all memory banks, all network
# interfaces, etc. individual devices are not logged.
#

CASE_INFO_FILE_NAME = 'case-info.txt'
METRICS_FILE_NAME = 'metrics.csv'
METRICS_VERSION = 2
FIELDNAMES = [
    'index',
    'step',
    'timestamp',
    'version',
    'cpu_user',
    'cpu_system',
    'cpu_user_system',
    'cpu_idle',
    'cpu_iowait',
    'memory_ram',
    'memory_swap',
    'memory_ram_swap',
    'disk_read_count',
    'disk_write_count',
    'disk_read_bytes',
    'disk_write_bytes',
    'disk_read_time',
    'disk_write_time',
    'disk_busy_time',
    'network_received_count',
    'network_sent_count',
    'network_received_bytes',
    'network_sent_bytes',
    'network_received_error',
    'network_sent_error',
    'network_received_drop',
    'network_sent_drop'
]
ROUND = 4

def _collect_metrics(stop_event: Event, metrics_path: str,
                     sample_interval: float, initial_timestamp: float,
                     initial_cpu: dict, initial_ram: int, initial_swap: int,
                     initial_disk_io: dict, initial_network_io: dict):
    global step_id
    index = 1

    # Create metrics file
    with open(metrics_path, 'w') as f:
        writer = DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()

        # Initial values
        row = {
            'index': index,
            'step': step_id,
            'timestamp': 0.0,
            'version': METRICS_VERSION,
            'cpu_user': 0.0,
            'cpu_system': 0.0,
            'cpu_user_system': 0.0,
            'cpu_idle': 0.0,
            'cpu_iowait': 0.0,
            'memory_ram': 0,
            'memory_swap': 0,
            'memory_ram_swap': 0,
            'disk_read_count': 0,
            'disk_write_count': 0,
            'disk_read_bytes': 0,
            'disk_write_bytes': 0,
            'disk_read_time': 0,
            'disk_write_time': 0,
            'disk_busy_time': 0,
            'network_received_count': 0,
            'network_sent_count': 0,
            'network_received_bytes': 0,
            'network_sent_bytes': 0,
            'network_received_error': 0,
            'network_sent_error': 0,
            'network_received_drop': 0,
            'network_sent_drop': 0
        }
        writer.writerow(row)
        index += 1
        sleep(sample_interval - (initial_timestamp - time()))

        while not stop_event.wait(0):
            # Collect metrics
            timestamp = time()
            cpu = psutil.cpu_times()
            ram = psutil.virtual_memory()
            swap = psutil.swap_memory()
            disk_io = psutil.disk_io_counters()
            network_io = psutil.net_io_counters()

            # Write to file
            row = {
                'index': index,
                'step': step_id,
                'timestamp': round(timestamp - initial_timestamp, ROUND),
                'version': METRICS_VERSION,
                'cpu_user': round(cpu.user - initial_cpu.user, ROUND),
                'cpu_system': round(cpu.system - initial_cpu.system, ROUND),
                'cpu_user_system': round((cpu.user - initial_cpu.user) + (cpu.system - initial_cpu.system), ROUND),
                'cpu_idle': round(cpu.idle - initial_cpu.idle, ROUND),
                'cpu_iowait': round(cpu.iowait - initial_cpu.iowait, ROUND),
                'memory_ram': ram.used,
                'memory_swap': swap.used,
                'memory_ram_swap': ram.used + swap.used,
                'disk_read_count': disk_io.read_count - initial_disk_io.read_count,
                'disk_write_count': disk_io.write_count - initial_disk_io.write_count,
                'disk_read_bytes': disk_io.read_bytes - initial_disk_io.read_bytes,
                'disk_write_bytes': disk_io.write_bytes - initial_disk_io.write_bytes,
                'disk_read_time': disk_io.read_time - initial_disk_io.read_time,
                'disk_write_time': disk_io.write_time - initial_disk_io.write_time,
                'disk_busy_time': disk_io.busy_time - initial_disk_io.busy_time,
                'network_received_count': network_io.packets_recv - initial_network_io.packets_recv,
                'network_sent_count': network_io.packets_sent - initial_network_io.packets_sent,
                'network_received_bytes': network_io.bytes_recv - initial_network_io.bytes_recv,
                'network_sent_bytes': network_io.bytes_sent - initial_network_io.bytes_sent,
                'network_received_error': network_io.errin - initial_network_io.errin,
                'network_sent_error': network_io.errout - initial_network_io.errout,
                'network_received_drop': network_io.dropin - initial_network_io.dropin,
                'network_sent_drop': network_io.dropout - initial_network_io.dropout
            }
            writer.writerow(row)
            index += 1

            # Honor sample time, remove metrics logging overhead
            sleep(sample_interval - (timestamp - time()))

class Collector():
    def __init__(self, results_run_path: str, directory: str,
                 sample_interval: float, number_of_steps: int,
                 run_id: int):
        self._started: bool = False
        self._data_path: str = os.path.abspath(results_run_path)
        self._directory: str = os.path.abspath(directory)
        self._number_of_steps: int = number_of_steps
        self._stop_event: Event = Event()

        global step_id
        step_id = 1

        # Only Linux is supported
        if platform.system() != 'Linux':
            msg = f'"{platform.system()} is not supported as OS'
            print(msg, file=sys.stderr)
            raise ValueError(msg)

        # System information: OS, kernel, architecture
        system_hostname = 'UNKNOWN'
        system_os_name = 'UNKNOWN'
        system_os_version = 'UNKNOWN'
        try:
            system_os_name = platform.freedesktop_os_release()['NAME']
            system_os_version = platform.freedesktop_os_release()['VERSION']
        except (OSError, KeyError):
            print(f'Cannot extract Freedesktop OS release data',
                  file=sys.stderr)
        system_hostname = platform.node()
        system_kernel = platform.platform()
        system_architecture = platform.uname().machine

        # CPU information: name, max frequency, core count
        cpu_name = 'UNKNOWN'
        try:
            raw = run(['lscpu'], capture_output=True)
            for line in raw.stdout.decode('utf-8').split('\n'):
                if 'Model name:' in line:
                    cpu_name = line.split(':')[1].strip()
                    break
        except CalledProcessError as e:
            print(f'Unable to determine CPU processor name: {e}',
                  file=sys.stderr)

        cpu_cores = psutil.cpu_count()
        cpu_min_freq = psutil.cpu_freq().min
        cpu_max_freq = psutil.cpu_freq().max

        # Memory information: RAM total, SWAP total
        memory_total = psutil.virtual_memory().total
        swap_total = psutil.swap_memory().total

        # Disk IO: name
        disk_partitions = {}
        for disk in psutil.disk_partitions():
            # Skip Docker's overlayFS
            if disk.fstype and 'docker' not in disk.mountpoint:
                disk_partitions[disk.mountpoint] = \
                        psutil.disk_usage(disk.mountpoint).total

        # Network IO: name, speed, MTU
        network_interfaces = psutil.net_if_stats()

        # Docker daemon: version, storage driver, cgroupfs
        client = DockerClient()
        docker_info = client.info()

        # Write machine information to disk
        with open(os.path.join(self._data_path, CASE_INFO_FILE_NAME), 'w') as f:
            f.write('===> CASE <===\n')
            f.write(f'Timestamp: {datetime.utcnow().isoformat()}\n')
            f.write(f'Directory: {directory}\n')
            f.write(f'Run: {run_id}\n')
            f.write(f'Number of steps: {self._number_of_steps}\n')
            f.write('\n')
            f.write('===> HARDWARE <===\n')
            f.write('System\n')
            f.write(f'\tHostname: {system_hostname}\n')
            f.write(f'\tOS name: {system_os_name}\n')
            f.write(f'\tOS version: {system_os_version}\n')
            f.write(f'\tKernel: {system_kernel}\n')
            f.write(f'\tArchitecture: {system_architecture}\n')
            f.write('CPU\n')
            f.write(f'\tName: {cpu_name}\n')
            f.write(f'\tCores: {cpu_cores}\n')
            f.write(f'\tMinimum frequency: {int(cpu_min_freq)} Hz\n')
            f.write(f'\tMaximum frequency: {int(cpu_max_freq)} Hz\n')
            f.write('Memory\n')
            f.write(f'\tRAM memory: {int(memory_total / 10 ** 6)} MB\n')
            f.write(f'\tSWAP memory: {int(swap_total / 10 ** 6)} MB\n')
            f.write('Storage\n')
            for disk, size in disk_partitions.items():
                f.write(f'\tDisk "{disk}": ' \
                        f'{round(size / 10 ** 9, 2)} GB\n')
            f.write('Network\n')
            for name, stats in network_interfaces.items():
                speed = stats.speed
                if speed == 0:
                    speed = 'UNKNOWN'

                f.write(f'\tInterface "{name}": {speed} mbps\n')
            f.write('\n')
            f.write('===> DOCKER <===\n')
            f.write(f'Version: {docker_info["ServerVersion"]}\n')
            f.write(f'Root directory: {docker_info["DockerRootDir"]}\n')
            f.write('Drivers:\n')
            f.write(f'\tStorage: {docker_info["Driver"]}\n')
            f.write(f'\tCgroupFS: {docker_info["CgroupDriver"]} ' + \
                    f'v{docker_info["CgroupVersion"]}\n')

        # Set initial metric values and start collection thread
        metrics_path = os.path.join(results_run_path, METRICS_FILE_NAME)
        initial_timestamp = time()
        initial_cpu = psutil.cpu_times()
        initial_ram = psutil.virtual_memory().used
        initial_swap = psutil.swap_memory().used
        initial_disk_io = psutil.disk_io_counters()
        initial_network_io = psutil.net_io_counters()
        self._thread: Thread = Thread(target=_collect_metrics,
                                      daemon=True,
                                      args=(self._stop_event,
                                            metrics_path,
                                            sample_interval,
                                            initial_timestamp,
                                            initial_cpu,
                                            initial_ram,
                                            initial_swap,
                                            initial_disk_io,
                                            initial_network_io))
        self._thread.start()

    @property
    def started(self):
        return self._started

    @property
    def name(self):
        return self.__name__

    def next_step(self):
        global step_id
        step_id += 1

        msg = f'Step ({step_id}) is higher than number of steps ' + \
              f'({self._number_of_steps})'
        assert (step_id <= self._number_of_steps), msg

    def stop(self):
        self._stop_event.set()

