#!/usr/bin/env python3

import os
import configparser
from container import Container

VERSION = '3.12.5'

class MorphRDB(Container):
    def __init__(self, data_path: str, verbose: bool):
        self._data_path = os.path.abspath(data_path)
        self._verbose = verbose
        super().__init__(f'kg-construct/morph-rdb:v{VERSION}', 'Morph-RDB',
                         volumes=[f'{self._data_path}/shared:/data/shared',
                                  f'{self._data_path}/morphrdb:/data'])

    def root_mount_directory(self) -> str:
        return __name__.lower()

    def execute(self, arguments) -> bool:
        return self.run(f'bash run-docker.sh /data/config.properties')

    def execute_mapping(self, mapping_file: str, output_file: str,
                        serialization: str, rdb_username: str = None,
                        rdb_password: str = None, rdb_host: str = None,
                        rdb_port: str = None, rdb_name: str = None,
                        rdb_type: str = None) -> bool:

        if serialization == 'nquads':
            serialization = 'N-QUADS'
        elif serialization == 'ntriples':
            serialization = 'N-TRIPLE'
        else:
            raise NotImplementedError('Unsupported serialization: '
                                      f'"{serialization}"')

        # Generate INI configuration file since no CLI is available
        config = configparser.ConfigParser()
        config = {
            'mappingdocument.file.path': mapping_file,
            'output.file.path': output_file,
            'output.rdflanguage': serialization,
        }

        if rdb_username is not None and rdb_password is not None \
            and rdb_host is not None and rdb_port is not None \
            and rdb_name is not None and rdb_type is not None:
            config['database.name[0]'] = rdb_name
            if rdb_type == 'MySQL':
                config['database.driver[0]'] = 'com.mysql.jdbc.Driver'
                config['database.type[0]'] = 'mysql'
                config['database.url[0]'] = f'jdbc:mysql://{rdb_host}:{rdb_port}/{rdb_name}'
            elif rdb_type == 'PostgreSQL':
                config['database.driver[0]'] = 'org.postgresql.Driver'
                config['database.type[0]'] = 'postgresql'
                config['database.url[0]'] = f'jdbc:postgresql://{rdb_host}:{rdb_port}/{rdb_name}'
            else:
                raise ValueError(f'Unknown RDB type: "{rdb_type}"')
            config['database.user[0]'] = rdb_username
            config['database.pwd[0]'] = rdb_password

        with open(os.path.join(self._data_path, 'morphrdb', 'config.properties'), 'w') as f:
            config.write(f)

        return self.execute([])
