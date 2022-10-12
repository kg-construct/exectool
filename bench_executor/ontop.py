#!/usr/bin/env python3

import os
import configparser
from container import Container

class Ontop(Container):
    def __init__(self, name, data_path):
        super().__init__(f'kg-construct/ontop:v{VERSION}', name,
                         ports={'8888':'8888'},
                         volumes=[f'{_data_path}/ontop:/data'])
        self._data_path = data_path

    def execute(self, mode, arguments):
        self.run(f'{mode} {" ".join(arguments)}')

    def execute_mapping(self, mode, config_file, arguments, mapping_file,
                        output_file, rdb_username: str = None,
                        rdb_password: str = None, rdb_host: str = None,
                        rdb_port: str = None, rdb_name: str = None,
                        rdb_type: str = None):
        arguments.append('-m')
        arguments.append(mapping_file)
        arguments.append('-o')
        arguments.append(output_file)
        arguments.append('-p')
        arguments.append(config_file)

        # Generate INI configuration file since no CLI is available
        if rdb_username is not None and rdb_password is not None \
            and rdb_host is not None and rdb_port is not None \
            and rdb_name is not None and rdb_type is not None:
            config = configparser.ConfigParser()
            config = {
                'jdbc.user' = rdb_user,
                'jdbc.password' = rdb_password
            }
            if rdb_type == 'MySQL':
                config['jdbc.url'] = 'jdbc:mysql://{rdb_host}:{rdb_port}/{rdb_name}'
                config['jdbc.driver'] = 'com.mysql.cj.jdbc.Driver'
            elif rdb_type == 'PostgreSQL':
                config['jdbc.url'] = 'jdbc:postgresql://{rdb_host}:{rdb_port}/{rdb_name}'
                config['jdbc.driver'] = 'org.postgresql.Driver'
            else:
                raise ValueError(f'Unknown RDB type: "{rdb_type}"')
        else:
            raise ValueError('Ontop only supports RDBs')

        self.execute(mode, arguments)

class OntopVirtualize(Ontop):
    def __init__(self, data_path: str):
        super().__init__('Ontop-Virtualize', data_path)

    def execute(self, arguments):
        super().execute('endpoint', arguments)

    def execute_mapping(self, mapping_file, output_file, serialization,
                        rdb_username: str = None, rdb_password: str = None,
                        rdb_host: str = None, rdb_port: str = None,
                        rdb_name: str = None, rdb_type: str = None):
        config_file = f'{self._data_path}/ontopvirtualize/config.properties']
        arguments = ['--cors-allowed-origins=*', '--port=8888']
        super().execute_mapping('endpoint', config_file, arguments,
                                mapping_file, output_file, rdb_username,
                                rdb_password, rdb_host, rdb_port, rdb_name,
                                rdb_type)

class OntopMaterialize(Ontop):
    def __init__(self, data_path: str):
        super().__init__('Ontop-Materialize', data_path)

    def execute(self, arguments):
        super().execute('materialize', arguments)

    def execute_mapping(self, mapping_file, output_file, serialization,
                        rdb_username: str = None, rdb_password: str = None,
                        rdb_host: str = None, rdb_port: str = None,
                        rdb_name: str = None, rdb_type: str = None):
        config_file = f'{self._data_path}/ontopmaterialize/config.properties']
        arguments = [ '-f', serialization ]
        super().execute_mapping('materialize', config_file, arguments,
                                mapping_file, output_file, rdb_username,
                                rdb_password, rdb_host, rdb_port, rdb_name,
                                rdb_type)
