#!/usr/bin/env python3

import os
import configparser
from container import Container

VERSION = '2.2.0'

class MorphKGC(Container):
    def __init__(self, data_path: str):
        super().__init__(f'kg-construct/morph-kgc:v{VERSION}', 'Morph-KGC',
                         volumes=[f'{data_path}/morphkgc:/data'])
        self._data_path = data_path

    def execute(self, arguments):
        self.run(f'python3 -m morph_kgc /data/config.ini')
        for line in self.logs():
            print(str(line.strip()))

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
            'output_file': output_file,
            'output_format': serialization
        }
        config['DataSource0'] = {
            'mappings': mapping_file
        }

        if rdb_username is not None and rdb_password is not None \
            and rdb_host is not None and rdb_port is not None \
            and rdb_name is not None and rdb_type is not None:
            arguments.append('-u')
            arguments.append(rdb_username)
            arguments.append('-p')
            arguments.append(rdb_password)
            if rdb_type == 'MySQL':
                protocol = 'mysql+pymysql'
            elif rdb_type == 'PostgreSQL':
                protocol = 'postgresql+psycopg2'
            else:
                raise ValueError(f'Unknown RDB type: "{rdf_type}"')
            rdb_dsn = f'{protocol}://{rdb_username}:{rdb_password}@{rdb_host}:{rdb_port}/{rdb_name}'
            config['DataSource0']['db_url'] = rdb_dsn

        with open(os.path.join(self._data_path, 'morphkgc', 'config.ini'), 'w') as f:
            config.write(f)

        self.execute([])
