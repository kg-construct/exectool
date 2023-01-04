#!/usr/bin/env python3

import os
import platform
import shutil
import psutil
from datetime import datetime
from subprocess import run, CalledProcessError
from threading import Thread, Event

#
# Hardware information is logged to 'hwinfo.txt' on construction.
#
# All data are stored in a CSV as 'stats.csv'.
# These data are accumulated among all CPU cores, all memory banks, all network
# interfaces, etc. individual devices are not logged.
#

HWINFO_FILE='hwinfo.txt'

class Collector():
    def __init__(self, data_path: str, main_directory: str, stop_event: Event):
        self._started: bool = False
        self._data_path = os.path.abspath(data_path)
        self._main_directory = os.path.abspath(main_directory)
        self._stop_event = stop_event
        self._thread: Thread = Thread(target=self._collect_metrics,
                                      daemon=True)
        self._cpu_count = psutil.cpu_count()
        self._cpu_max_freq = psutil.cpu_freq()

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
        system_kernel = platform.uname().release
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
            if disk.fstype:
                disk_partitions[disk.mountpoint] = \
                        psutil.disk_usage(disk.mountpoint).total

        # Network IO: name, speed, MTU
        network_interfaces = psutil.net_if_stats()

        # Write machine information to disk
        with open(os.path.join(self._data_path, HWINFO_FILE), 'w') as f:
            f.write('MACHINE INFORMATION\n')
            f.write('===> Cases\n')
            f.write(f'\tTimestamp: {datetime.utcnow().isoformat()}\n')
            f.write(f'\tRoot: {main_directory}\n')
            f.write('===> System\n')
            f.write(f'\tHostname: {system_hostname}\n')
            f.write(f'\tOS name: {system_os_name}\n')
            f.write(f'\tOS version: {system_os_version}\n')
            f.write(f'\tKernel: Linux {system_kernel}\n')
            f.write(f'\tArchitecture: {system_architecture}\n')
            f.write('===> CPU\n')
            f.write(f'\tName: {cpu_name}\n')
            f.write(f'\tCores: {cpu_cores}\n')
            f.write(f'\tMinimum frequency: {int(cpu_min_freq)} Hz\n')
            f.write(f'\tMaximum frequency: {int(cpu_max_freq)} Hz\n')
            f.write('===> Memory\n')
            f.write(f'\tRAM memory: {int(memory_total / 10 ** 6)} MB\n')
            f.write(f'\tSWAP memory: {int(swap_total / 10 ** 6)} MB\n')
            f.write('===> Storage\n')
            for disk, size in disk_partitions.items():
                f.write(f'\tDisk "{disk}": ' \
                        f'{round(size / 10 ** 9, 2)} GB\n')
            f.write('===> Network\n')
            for name, stats in network_interfaces.items():
                f.write(f'\tInterface "{name}": {stats.speed} mbps\n')

    def started(self):
        return self._started

    @property
    def name(self):
        return self.__name__

    def _collect_metrics(self):
        cpu_times = psutil.cpu_times()
        memory = psutil.virtual_memory()
        swap = psutil.swap_memory()
        disk_io = psutil.disk_io_counters()
        network_io = psutil.network_io_counters()
