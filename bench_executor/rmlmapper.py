#!/usr/bin/env python3

import os
import psutil
from container import Container

VERSION = '6.0.0'

class RMLMapper(Container):
    def __init__(self, data_path: str, config_path: str, verbose: bool):
        self._data_path = os.path.abspath(data_path)
        self._config_path = os.path.abspath(config_path)
        os.makedirs(os.path.join(self._data_path, 'rmlmapper'), exist_ok=True)
        super().__init__(f'blindreviewing/rmlmapper:v{VERSION}', 'RMLMapper',
                         verbose,
                         volumes=[f'{self._data_path}/rmlmapper:/data',
                                  f'{self._data_path}/shared:/data/shared'])

    @property
    def root_mount_directory(self) -> str:
        return __name__.lower()

    def execute(self, arguments: list) -> bool:
        if self._verbose:
            arguments.append('-vvvvvvvvvvv')

        self._logs.append(f'Executing RMLMapper with arguments '
                          f'{" ".join(arguments)}\n')

        # Set Java heap to 1/2 of available memory instead of the default 1/4
        max_heap = int(psutil.virtual_memory().total * (1/2))

        # Execute command
        cmd = f'java -Xmx{max_heap} -Xms{max_heap} ' + \
              f'-jar rmlmapper/rmlmapper.jar ' + \
              f'{" ".join(arguments)}'
        return self.run_and_wait_for_exit(cmd)

    def execute_mapping(self, mapping_file, output_file, serialization,
                        rdb_username: str = None, rdb_password: str = None,
                        rdb_host: str = None, rdb_port: str = None,
                        rdb_name: str = None, rdb_type: str = None) -> bool:
        arguments = ['-m', os.path.join('/data/shared/', mapping_file),
                     '-s', serialization,
                     '-o', os.path.join('/data/shared/', output_file),
                     '-d'] # Enable duplicate removal

        if rdb_username is not None and rdb_password is not None \
            and rdb_host is not None and rdb_port is not None \
            and rdb_name is not None and rdb_type is not None:

            arguments.append('-u')
            arguments.append(rdb_username)
            arguments.append('-p')
            arguments.append(rdb_password)

            parameters = ''
            if rdb_type == 'MySQL':
                protocol = 'jdbc:mysql'
                parameters = '?allowPublicKeyRetrieval=true&useSSL=false'
            elif rdb_type == 'PostgreSQL':
                protocol = 'jdbc:postgresql'
            else:
                raise ValueError(f'Unknown RDB type: "{rdf_type}"')
            rdb_dsn = f'{protocol}://{rdb_host}:{rdb_port}/' + \
                      f'{rdb_name}{parameters}'
            arguments.append('-dsn')
            arguments.append(rdb_dsn)

        return self.execute(arguments)
