#!/usr/bin/env python3

import docker
from time import sleep
WAIT_TIME = 1

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

    def run(self, command: str = '', detach=True):
        self._container = self._client.containers.run(self._container_name,
                                                      command, name=self._name,
                                                      detach=detach, remove=True,
                                                      ports=self._ports,
                                                      environment=self._environment,
                                                      volumes=self._volumes)

    def logs(self):
        return self._container.logs(stream=True)

    def run_and_wait_for_log(self, log_line: str, command: str =''):
        print(f'Starting "{self._name}" ({self._container_name}): "{command}"')
        self.run(command)

        for line in self.logs():
            line = str(line.strip())
            print(line)
            if log_line in line:
                sleep(WAIT_TIME)
                print(f'Container "{self._name}" running!')
                return

    def stop(self):
        self._container.stop()

