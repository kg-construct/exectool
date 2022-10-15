#!/usr/bin/env python3

import os
import configparser
from container import Container

VERSION = '4.5.7.1'

class SDMRDFizer(Container):
    def __init__(self, data_path: str, verbose: bool):
        self._data_path = os.path.abspath(data_path)
        self._verbose = verbose
        super().__init__(f'kg-construct/sdm-rdfizer:v{VERSION}', 'SDM-RDFizer',
                         volumes=[f'{self._data_path}/sdmrdfizer:/data',
                                  f'{self._data_path}/shared:/data/shared'])

    def root_mount_directory(self) -> str:
        return __name__.lower()

    def execute(self, arguments) -> bool:
        return self.run(f'python3 sdm-rdfizer/rdfizer/run_rdfizer.py '
                        '/data/config.ini')

    def execute_mapping(self, mapping_file: str, output_file: str,
                        serialization: str, rdb_username: str = None,
                        rdb_password: str = None, rdb_host: str = None,
                        rdb_port: str = None, rdb_name: str = None,
                        rdb_type: str = None) -> bool:

        name = os.path.splitext(os.path.basename(output_file))[0]
        config = configparser.ConfigParser()
        config['default'] = {
            'main_directory': '/data'
        }
        config['datasets'] = {
            'number_of_datasets': 1,
            'output_folder': '/data',
            'all_in_one_file': 'yes',
            'remove_duplicate': 'yes',
            'enrichment': 'yes',
            'name': name,
            'ordered': 'no',
            'large_file': 'false'
        }
        config['dataset1'] = {
            'name': name,
            'mapping': mapping_file
        }

        if rdb_username is not None and rdb_password is not None \
            and rdb_host is not None and rdb_port is not None \
            and rdb_name is not None and rdb_type is not None:
            config['dataset1']['user'] = rdb_username
            config['dataset1']['password'] = rdb_password
            config['dataset1']['host'] = rdb_host
            config['dataset1']['port'] = rdb_port
            config['dataset1']['db'] = rdb_name
            if rdb_type == 'MySQL':
                config['datasets']['dbType'] = 'mysql'
            elif rdb_type == 'PostgreSQL':
                config['datasets']['dbType'] = 'postgresql'
            else:
                raise NotImplementedError('SDM-RDFizer does not support RDB '
                                          f'"{rdb_type}"')

        with open(os.path.join(self._data_path, 'sdmrdfizer', 'config.ini'), 'w') as f:
            config.write(f)

        return self.execute([])
