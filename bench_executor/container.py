#!/usr/bin/env python3

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
        self._long_id = None
        self._cgroups_mode = None
        self._cgroups_dir = None

        # create network if not exist
        try:
            self._client.networks.get(NETWORK_NAME)
        except docker.errors.NotFound:
            self._client.networks.create(NETWORK_NAME)

        # detect CGroups location
        if os.path.exists(CGROUPS_DIR_SYSTEMD_V1):
            self._cgroups_dir = CGROUPS_DIR_SYSTEMD_V1
            self._cgroups_mode = CGROUPS_MODE_SYSTEMD
        elif os.path.exists(CGROUPS_DIR_CGROUPFS_V1):
            self._cgroups_dir = CGROUPS_MODE_CGROUPSFS_V1
            self._cgroups_mode = CGROUPS_MODE_CGROUPSFS
        elif os.path.exists(CGROUPS_DIR_SYSTEMD_V2):
            self._cgroups_dir = CGROUPS_DIR_SYSTEMD_V2
            self._cgroups_mode = CGROUPS_MODE_SYSTEMD
        elif os.path.exists(CGROUPS_DIR_CGROUPFS_V2):
            self._cgroups_dir = CGROUPS_DIR_CGROUPSFS_V2
            self._cgroups_mode = CGROUPS_MODE_CGROUPSFS
        else:
            print('CGroups not found, stats unsupported', file=sys.stderr)

    def is_running(self):
        if self._container is not None:
            return self._container.status == 'running'
        return False

    def is_exited(self):
        if self._container is not None:
            return self._container.status == 'exited'
        return False

    def run(self, command: str = '', detach=True) -> bool:
        try:
            self._container = self._client.containers.run(self._container_name,
                                                          command,
                                                          name=self._name,
                                                          detach=detach,
                                                          #auto_remove=True,
                                                          #remove=True,
                                                          ports=self._ports,
                                                          network=NETWORK_NAME,
                                                          environment=self._environment,
                                                          volumes=self._volumes)
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

        print(f'Retrieving container "{self._name}" logs failed!', file=sys.stderr)
        return None

    def run_and_wait_for_log(self, log_line: str, command: str ='') -> bool:
        if not self.run(command):
            print(f'Command "{command}" failed')
            return False

        start = time()
        logs = self._container.logs(stream=True, follow=True)
        if logs is not None:
            for line in logs:
                line = line.strip().decode()

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
        # Docker API is slow for retrieving stats, use underlying cgroups
        # instead to retrieve the raw data without the processing overhead of
        # the Docker daemon
        if self._container is not None and self._cgroups_dir is not None:
            stats = {
                'time': round(time(), 2),
                'cpu': {},
                'memory': {},
                'io': {},
                'network': {}
            }
            long_id = self._container.id
            path = None

            if self._cgroups_mode == CGROUPS_MODE_SYSTEMD:
                path = os.path.join(self._cgroups_dir,
                                    f'docker-{long_id}.scope')
            elif self._cgroups_mode == CGROUPS_MODE_CGROUPSFS:
                path = os.path.join(self._cgroups_dir, long_id)
            else:
                raise ValueError('Unknown CGroup mode, this statement should '
                                 'be unreachable. Please report this as a bug.')

            if not os.path.exists(path):
                if not silence_failure:
                    print('Container CGroupFS file does not exist (yet)',
                          file=sys.stderr)
                return None

            try:
                with open(os.path.join(path, 'cpu.stat'), 'r') as f:
                    # <metric> <value>
                    cpu_raw = f.read()
                    for raw in cpu_raw.split('\n'):
                        if 'usage_usec' in raw or 'user_usec' in raw \
                            or 'system_usec' in raw:
                            metric, value = raw.split(' ')
                            if metric == 'usage_usec':
                                stats['cpu']['total_cpu_time'] = int(value) / (10**6)
                            elif metric == 'user_usec':
                                stats['cpu']['user_cpu_time'] = int(value) / (10**6)
                            elif metric == 'system_usec':
                                stats['cpu']['system_cpu_time'] = int(value) / (10**6)

                with open(os.path.join(path, 'memory.current'), 'r') as f:
                    # <value>
                    memory_raw = f.read()
                    stats['memory']['total'] = int(memory_raw) / (10**6)

                with open(os.path.join(path, 'io.stat'), 'r') as f:
                    # <major:minor> rbytes=<value> wbytes=<value> rios=<value>
                    # wios=<value> dbytes=<value> dios=<value>
                    io_raw = f.read()
                    for raw in io_raw.split('\n'):
                        if raw == '':
                            continue
                        raw = raw.split(' ')
                        device = os.path.realpath(os.path.join(DEV_BLOCK_DIR,
                                                               raw[0]))
                        bytes_read = int(raw[1].split('=')[1])
                        bytes_write = int(raw[2].split('=')[1])
                        number_of_reads = int(raw[3].split('=')[1])
                        number_of_writes = int(raw[4].split('=')[1])
                        bytes_discarded = int(raw[5].split('=')[1])
                        number_of_discards = int(raw[6].split('=')[1])
                        stats['io'][device] = {
                            'bytes_read': bytes_read / (10**6),
                            'bytes_write': bytes_write / (10**6),
                            'bytes_discarded': bytes_discarded / (10**6),
                            'number_of_reads': number_of_reads,
                            'number_of_writes': number_of_writes,
                            'number_of_discards': number_of_discards
                        }

                if cpu_raw and memory_raw and io_raw:
                    return stats
            except FileNotFoundError:
                return None

        # Metrics measurement threads may try to race for a metrics dump
        # a soon as the container is started by polling this continuously.
        # Silence the error message in such cases to avoid log spam
        if not silence_failure:
            print(f'Retrieving container "{self._name}" stats failed!', file=sys.stderr)
        return None
