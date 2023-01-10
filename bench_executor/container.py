#!/usr/bin/env python3

import re
import os
import sys
import docker
from time import time, sleep
from typing import Optional, List, Tuple
try:
    from bench_executor import Logger
except ModuleNotFoundError:
    from logger import Logger

WAIT_TIME = 1 # seconds
TIMEOUT_TIME = 600 # seconds
NETWORK_NAME = 'bench_executor'

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

    def create_network(self, name: str):
        try:
            self._client.networks.get(name)
        except docker.errors.NotFound:
            self._client.networks.create(name)


class Container():
    def __init__(self, container: str, name: str, logger: Logger,
                 ports: dict = {}, environment: dict = {}, volumes: dict = {}):
        self._manager = ContainerManager()
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
        self._logger = logger

        # create network if not exist
        self._manager.create_network(NETWORK_NAME)

    @property
    def started(self):
        return self._started

    @property
    def name(self):
        return self._name

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
            for line in logs.split('\n'):
                self._logger.debug(line.strip())
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
                line = line.decode().strip()
                self._logger.debug(line)

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
                line = line.decode().strip()
                self._logger.debug(line)

        if self._container.wait()['StatusCode'] == 0:
            return True

        return False

    def stop(self) -> bool:
        try:
            if self._container is not None:
                self._container.stop()
                self._container.remove(v=True)
            return True
        # Containers which are already stopped will raise an error which we can
        # ignore
        except docker.errors.APIError as e:
            pass

        return True
