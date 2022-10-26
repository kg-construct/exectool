#!/usr/bin/env python3

import re
import os
import sys
import docker
from time import time, sleep
from typing import Optional, List, Tuple

WAIT_TIME = 1 # seconds
TIMEOUT_TIME = 100 # seconds
NETWORK_NAME = 'bench_executor'
DEV_BLOCK_DIR = '/dev/block/'
CGROUPS_DIR_SYSTEMD_V1 = '/sys/fs/cgroup/memory/system.slice/'
CGROUPS_DIR_CGROUPFS_V1 = '/sys/fs/cgroup/memory/docker/'
CGROUPS_DIR_SYSTEMD_V2 = '/sys/fs/cgroup/system.slice/'
CGROUPS_DIR_CGROUPFS_V2 = '/sys/fs/cgroup/docker/'
CGROUPS_MODE_SYSTEMD = 'systemd'
CGROUPS_MODE_CGROUPSFS = 'cgroupfs'
NETWORK_REGEX = r"(\w*):\s*(\d*)\s*(\d*)\s*(\d*)\s*(\d*)\s*(\d*)\s*(\d*)" + \
                r"\s*(\d*)\s*(\d*)\s*(\d*)\s*(\d*)\s*(\d*)\s*(\d*)\s*(\d*)" + \
                r"\s*(\d*)\s*(\d*)\s*(\d*)"

class ContainerManager():
    def __init__(self):
        self._client = docker.from_env()

    def list_all(self):
        return self._client.containers.list(all=True)

    def stop_all(self):
        stopped = False
        removed = False

        for container in self.list_all():
            try:
                container.stop()
                stopped = True
            except docker.errors.APIError:
                pass
            try:
                container.remove()
                removed = True
            except docker.errors.APIError:
                pass
            print(f'Container {container.name}: stopped: {stopped} '
                  f'removed: {removed}')

class Container():
    def __init__(self, container: str, name: str, ports: dict = {},
                 environment: dict = {}, volumes: dict = {}):
        self._client = docker.from_env()
        self._container = None
        self._container_name = container
        self._name = name
        self._ports = ports
        self._volumes = volumes
        self._environment = environment
        self._logs = []
        self._proc_pid = None
        self._long_id = None
        self._cgroups_mode = None
        self._cgroups_dir = None
        self._started = False

        # create network if not exist
        try:
            self._client.networks.get(NETWORK_NAME)
        except docker.errors.NotFound:
            self._client.networks.create(NETWORK_NAME)

        # detect CGroups location
        if os.path.exists(CGROUPS_DIR_SYSTEMD_V1):
            self._cgroups_dir = CGROUPS_DIR_SYSTEMD_V1
            self._cgroups_mode = CGROUPS_MODE_SYSTEMD
            raise NotImplementedError('CGroupsFSv1 SystemD driver is '
                                      'unsupported, use CGroupFSv2 SystemD '
                                      'driver instead')
        elif os.path.exists(CGROUPS_DIR_CGROUPFS_V1):
            self._cgroups_dir = CGROUPS_MODE_CGROUPSFS_V1
            self._cgroups_mode = CGROUPS_MODE_CGROUPSFS
            raise NotImplementedError('CGroupsFSv1 plain driver is unsupported,'
                                      ' use CGroupFSv2 SystemD driver instead')
        elif os.path.exists(CGROUPS_DIR_SYSTEMD_V2):
            self._cgroups_dir = CGROUPS_DIR_SYSTEMD_V2
            self._cgroups_mode = CGROUPS_MODE_SYSTEMD
        elif os.path.exists(CGROUPS_DIR_CGROUPFS_V2):
            self._cgroups_dir = CGROUPS_DIR_CGROUPSFS_V2
            self._cgroups_mode = CGROUPS_MODE_CGROUPSFS
            raise NotImplementedError('CGroupsFSv2 plain driver is unsupported,'
                                      'use CGroupFSv2 SystemD driver instead')
        else:
            print('CGroups not found, stats unsupported', file=sys.stderr)

    @property
    def started(self):
        return self._started

    def run(self, command: str = '', detach=True) -> bool:
        try:
            e = self._environment
            self._container = self._client.containers.run(self._container_name,
                                                          command,
                                                          name=self._name,
                                                          detach=detach,
                                                          ports=self._ports,
                                                          network=NETWORK_NAME,
                                                          environment=e,
                                                          volumes=self._volumes)
            self._started = (self._container is not None)
            return True
        except docker.errors.APIError as e:
            print(e, file=sys.stderr)

        print(f'Starting container "{self._name}" failed!', file=sys.stderr)
        return False

    def exec(self, command: str) -> Tuple[bool, List[str]]:
        logs = None

        try:
            exit_code, output = self._container.exec_run(command)
            logs = output.decode()
            if self._verbose:
                for line in logs.split('\n'):
                    print(line)
            if exit_code == 0:
                return True, logs
        except docker.errors.APIError as e:
            print(e, file=sys.stderr)

        return False, logs

    def logs(self) -> Optional[List[str]]:
        try:
            for line in self._container.logs(stream=True, follow=False):
                self._logs.append(line.decode())

            return self._logs
        except docker.errors.APIError as e:
            print(e, file=sys.stderr)

        print(f'Retrieving container "{self._name}" logs failed!',
              file=sys.stderr)
        return None

    def run_and_wait_for_log(self, log_line: str, command: str ='') -> bool:
        if not self.run(command):
            print(f'Command "{command}" failed')
            return False

        start = time()
        logs = self._container.logs(stream=True, follow=True)
        if logs is not None:
            for line in logs:
                line = line.decode()
                self._logs.append(line)
                line = line.strip()

                if time() - start > TIMEOUT_TIME:
                    print(f'Starting container "{self._name}" timed out!',
                          file=sys.stderr)
                    return False

                if log_line in line:
                    sleep(WAIT_TIME)
                    return True

        print(f'Waiting for container "{self._name}" failed!',
              file=sys.stderr)
        return False

    def run_and_wait_for_exit(self, command: str = '') -> bool:
        if not self.run(command):
            return False

        logs = self._container.logs(stream=True, follow=True)
        if logs is not None:
            for line in logs:
                line = line.decode()
                self._logs.append(line)

        if self._container.wait()['StatusCode'] == 0:
            return True

        return False

    def stop(self) -> bool:
        try:
            if self._container is not None:
                self._container.stop()
                self._container.remove()
            return True
        # Containers which are already stopped will raise an error which we can
        # ignore
        except docker.errors.APIError as e:
            pass

        return True

    def stats(self, silence_failure=False) -> Optional[dict]:
        # Docker API is slow for retrieving stats, use underlying cgroups and
        # proc file systems instead to retrieve the raw data without the
        # processing overhead of the Docker daemon
        if self._container is not None and self._cgroups_dir is not None:
            stats = {}

            # Get IDs to access metrics on filesystem
            if self._proc_pid is None or self._long_id is None:
                self._container.reload()
                pid = self._container.attrs['State']['Pid']
                if pid != 0:
                    self._proc_pid = pid
                self._long_id = self._container.id
            cgroup_path = None
            proc_path = None

            if self._cgroups_mode == CGROUPS_MODE_SYSTEMD:
                cgroup_path = os.path.join(self._cgroups_dir,
                                           f'docker-{self._long_id}.scope')
            elif self._cgroups_mode == CGROUPS_MODE_CGROUPSFS:
                cgroup_path = os.path.join(self._cgroups_dir, self._long_id)
            else:
                raise ValueError('Unknown CGroup mode, this statement should '
                                 'be unreachable. Please report this as a bug.')

            proc_path = os.path.join(f'/proc/{self._proc_pid}')

            if not os.path.exists(cgroup_path):
                if not silence_failure:
                    print(f'Container {self._cgroups_dir} files do not exist',
                          file=sys.stderr)
                return None

            if not os.path.exists(proc_path):
                if not silence_failure:
                    print('Container /proc files do not exist',
                          file=sys.stderr)
                return None

            try:
                p = os.path.join(cgroup_path, 'cpu.stat')
                with open(p, 'r') as f:
                    # <metric> <value>
                    cpu_raw = f.read()
                    for raw in cpu_raw.split('\n'):
                        if 'usage_usec' in raw and 'user_usec' in raw \
                            and 'system_usec' in raw:
                            metric, value = raw.split(' ')
                            if metric == 'usage_usec':
                                stats['cpu_total_time'] = \
                                    int(value) / (10**6)
                            elif metric == 'user_usec':
                                stats['cpu_user_time'] = \
                                    int(value) / (10**6)
                            elif metric == 'system_usec':
                                stats['cpu_system_time'] = \
                                    int(value) / (10**6)

                p = os.path.join(cgroup_path, 'memory.current')
                with open(p, 'r') as f:
                    # <value>
                    memory_raw = f.read()
                    stats['memory_total_size'] = int(memory_raw) / (10**3)

                p = os.path.join(cgroup_path, 'io.stat')
                stats['io'] = []
                with open(p, 'r') as f:
                    # <major:minor> rbytes=<value> wbytes=<value> rios=<value>
                    # wios=<value> dbytes=<value> dios=<value>
                    io_raw = f.read()
                    KEYS = ['rbytes', 'wbytes', 'rios', 'wios', 'dbytes',
                            'dios']
                    for raw in io_raw.split('\n'):
                        missing_key = False
                        for k in KEYS:
                            if k not in raw:
                                missing_key = True
                                break

                        if missing_key:
                            continue

                        device_number = re.search(r"(\d*:\d*) ",
                                                  raw).groups()[0]
                        device = os.path.realpath(os.path.join(DEV_BLOCK_DIR,
                                                               device_number))
                        bytes_read = int(re.search(r"rbytes=(\d*)",
                                                   raw).groups()[0])
                        bytes_write = int(re.search(r"wbytes=(\d*)",
                                                    raw).groups()[0])
                        number_of_reads = int(re.search(r"rios=(\d*)",
                                                        raw).groups()[0])
                        number_of_writes = int(re.search(r"wios=(\d*)",
                                                         raw).groups()[0])
                        bytes_discarded = int(re.search(r"dbytes=(\d*)",
                                                        raw).groups()[0])
                        number_of_discards = int(re.search(r"dios=(\d*)",
                                                           raw).groups()[0])
                        # Multiple disks will results in multiple list entries
                        stats['io'].append({
                            'device': device,
                            'total_size_read': bytes_read / (10**3),
                            'total_size_write': bytes_write / (10**3),
                            'total_size_discard': bytes_discarded / (10**3),
                            'number_of_read': number_of_reads,
                            'number_of_write': number_of_writes,
                            'number_of_discard': number_of_discards
                        })

                p = os.path.join(proc_path, 'net', 'dev')
                stats['network'] = []
                with open(p, 'r') as f:
                    # header Interface | Receive | Transmist
                    # header <metrics>
                    # <interface>: <received bytes> <received_packets>
                    # <received_errs> <received_drop> <received_fifo>
                    # <received_frame> <received_compressed>
                    # <received_multicast> <send_bytes> <send_packets>
                    # <send_errs> <send_drop> <send_fifo> <send_colls>
                    # <send_carrier> <send_compressed>
                    network_raw = f.read()
                    for raw in network_raw.split('\n'):
                        if raw == '' or 'Receive' in raw or 'packets' in raw:
                            continue
                        try:
                            network = re.search(NETWORK_REGEX, raw).groups()
                        except AttributeError:
                            continue

                        device = network[0]
                        bytes_received = int(network[1])
                        packets_received = int(network[2])
                        errors_received = int(network[3])
                        dropped_received = int(network[4])
                        fifo_received = int(network[5])
                        frame_received = int(network[6])
                        compressed_received = int(network[7])
                        multicast_received = int(network[8])
                        bytes_transmitted = int(network[9])
                        packets_transmitted = int(network[10])
                        errors_transmitted = int(network[11])
                        dropped_transmitted = int(network[12])
                        fifo_transmitted = int(network[13])
                        colls_transmitted = int(network[14])
                        carrier_transmitted = int(network[15])
                        compressed_transmitted = int(network[16])

                        # Multiple network interfaces will results
                        # in multiple list entries
                        stats['network'].append({
                            'device': device,
                            'total_size_received': bytes_received / (10**3),
                            'total_size_transmitted': bytes_transmitted / (10**3),
                            'number_of_packets_received': packets_received,
                            'number_of_packets_transmitted': packets_transmitted,
                            'number_of_errors_received': errors_received,
                            'number_of_errors_transmitted': errors_transmitted,
                            'number_of_drops_received': dropped_received,
                            'number_of_drops_transmitted': dropped_transmitted
                        })

                if cpu_raw and memory_raw and io_raw and network_raw:
                    return stats
            except FileNotFoundError:
                return None

        # Metrics measurement threads may try to race for a metrics dump
        # a soon as the container is started by polling this continuously.
        # Silence the error message in such cases to avoid log spam
        if not silence_failure:
            print(f'Retrieving container "{self._name}" stats failed!',
                  file=sys.stderr)
        return None

    @property
    def name(self):
        return self._name
