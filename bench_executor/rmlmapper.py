#!/usr/bin/env python3

import os
from container import Container

VERSION = '6.0.0'

class RMLMapper(Container):
    def __init__(self, data_path: str, verbose: bool):
        self._verbose = verbose
        self._data_path = os.path.abspath(data_path)
        super().__init__(f'kg-construct/rmlmapper:v{VERSION}', 'RMLMapper',
                         volumes=[f'{self._data_path}/rmlmapper:/data'])

    def execute(self, arguments: list) -> bool:
        if self._verbose:
            arguments.append('-vvvvvvvvvvv')
        success = self.run(f'java -jar rmlmapper/rmlmapper.jar {" ".join(arguments)}')

        return success

    def execute_mapping(self, mapping_file, output_file, serialization,
                        rdb_username: str = None, rdb_password: str = None,
                        rdb_host: str = None, rdb_port: str = None,
                        rdb_name: str = None, rdb_type: str = None) -> bool:
        arguments = ['-m', mapping_file,
                     '-s', serialization,
                     '-o', output_file]
        if rdb_username is not None and rdb_password is not None \
            and rdb_host is not None and rdb_port is not None \
            and rdb_name is not None and rdb_type is not None:
            arguments.append('-u')
            arguments.append(rdb_username)
            arguments.append('-p')
            arguments.append(rdb_password)
            if rdb_type == 'MySQL':
                protocol = 'jdbc:mysql'
            elif rdb_type == 'PostgreSQL':
                protocol = 'jdbc:postgresql'
            else:
                raise ValueError(f'Unknown RDB type: "{rdf_type}"')
            rdb_dsn = f'{protocol}://{rdb_host}:{rdb_port}/{rdb_name}'
            arguments.append('-dsn')
            arguments.append(rdb_dsn)

        return self.execute(arguments)
