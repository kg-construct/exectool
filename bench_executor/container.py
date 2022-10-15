#!/usr/bin/env python3

import sys
import docker
from time import time, sleep
from typing import Optional, List, Tuple

WAIT_TIME = 1 # seconds
TIMEOUT_TIME = 100 # seconds
NETWORK_NAME = 'bench_executor'

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

        # create network if not exist
        try:
            self._client.networks.get(NETWORK_NAME)
        except docker.errors.NotFound:
            self._client.networks.create(NETWORK_NAME)

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
                for line in logs:
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

    def stats(self) -> Optional[dict]:
        try:
            stats = self._container.stats(decode=False, stream=False)
            return dict(stats.decode())
        except docker.errors.APIError as e:
            print(e, file=sys.stderr)

        print(f'Retrieving container "{self._name}" stats failed!', file=sys.stderr)
        return None
