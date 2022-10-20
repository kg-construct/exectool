#!/usr/bin/env python3

import os
import configparser
from container import Container

VERSION = '2.2.0'

class MorphKGC(Container):
    def __init__(self, data_path: str, config_path: str, verbose: bool):
        self._data_path = os.path.abspath(data_path)
        self._config_path = os.path.abspath(config_path)
        self._verbose = verbose
        os.makedirs(os.path.join(self._data_path, 'morphkgc'), exist_ok=True)
        super().__init__(f'kg-construct/morph-kgc:v{VERSION}', 'Morph-KGC',
                         volumes=[f'{self._data_path}/morphkgc:/data',
                                  f'{self._data_path}/shared:/data/shared'])

    def root_mount_directory(self) -> str:
        return __name__.lower()

    def execute(self, arguments) -> bool:
        cmd = f'python3 -m morph_kgc /data/config_morphkgc.ini'
        return self.run_and_wait_for_exit(cmd)

    def execute_mapping(self, mapping_file: str, output_file: str,
                        serialization: str, rdb_username: str = None,
                        rdb_password: str = None, rdb_host: str = None,
                        rdb_port: str = None, rdb_name: str = None,
                        rdb_type: str = None):

        if serialization == 'nquads':
            serialization = 'N-QUADS'
        elif serialization == 'ntriples':
            serialization = 'N-TRIPLES'
        else:
            raise NotImplemented(f'Unsupported serialization:'
                                 f'"{serialization}"')

        # Generate INI configuration file since no CLI is available
        config = configparser.ConfigParser()
        config['CONFIGURATION'] = {
            'output_file': f'/data/shared/{output_file}',
            'output_format': serialization
        }
        config['DataSource0'] = {
            'mappings': f'/data/shared/{mapping_file}'
        }

        if rdb_username is not None and rdb_password is not None \
            and rdb_host is not None and rdb_port is not None \
            and rdb_name is not None and rdb_type is not None:
            if rdb_type == 'MySQL':
                protocol = 'mysql+pymysql'
            elif rdb_type == 'PostgreSQL':
                protocol = 'postgresql+psycopg2'
            else:
                raise ValueError(f'Unknown RDB type: "{rdf_type}"')
            rdb_dsn = f'{protocol}://{rdb_username}:{rdb_password}' + \
                      f'@{rdb_host}:{rdb_port}/{rdb_name}'
            config['DataSource0']['db_url'] = rdb_dsn

        os.makedirs(os.path.join(self._data_path, 'morphkgc'), exist_ok=True)
        path = os.path.join(self._data_path, 'morphkgc', 'config_morphkgc.ini')
        with open(path, 'w') as f:
            config.write(f)

        return self.execute([])
