#!/usr/bin/env python3

import os
import sys
import psutil
import configparser
from container import Container

VERSION = '4.2.0'

class _Ontop(Container):
    def __init__(self, name, data_path: str, verbose: bool, mode: str):
        self.verbose = verbose

        # Set Java heap to 1/2 of available memory instead of the default 1/4
        max_heap = int(psutil.virtual_memory().total * (1/2))

        environment = {'ONTOP_JAVA_ARGS': f'-Xmx{max_heap} -Xms{max_heap}'}
        super().__init__(f'kg-construct/ontop:v{VERSION}', name,
                         ports={'8888':'8888'},
                         environment=environment,
                         volumes=[f'{self._data_path}/ontop{mode}:/data',
                                  f'{self._data_path}/shared:/data/shared'])

    def root_mount_directory(self) -> str:
        return __name__.lower().replace('_', '')

    def execute(self, mode, arguments) -> bool:
        cmd = f'/ontop/ontop {mode} {" ".join(arguments)}'
        if mode == 'endpoint':
            log_line = 'OntopEndpointApplication - Started ' + \
                       'OntopEndpointApplication'
            success = self.run_and_wait_for_log(log_line, cmd)
        elif mode == 'materialize':
            success = self.run_and_wait_for_exit(cmd)
        else:
            print(f'Unknown Ontop mode "{mode}"', file=sys.stderr)
            success = False

        return success

    def execute_mapping(self, mode, config_file, arguments, mapping_file,
                        output_file, rdb_username: str = None,
                        rdb_password: str = None, rdb_host: str = None,
                        rdb_port: str = None, rdb_name: str = None,
                        rdb_type: str = None) -> bool:
        # Generate INI configuration file since no CLI is available
        if rdb_username is not None and rdb_password is not None \
            and rdb_host is not None and rdb_port is not None \
            and rdb_name is not None and rdb_type is not None:
            config = configparser.ConfigParser()
            config['root'] = {
                'jdbc.user': rdb_username,
                'jdbc.password': rdb_password
            }
            if rdb_type == 'MySQL':
                dsn = f'jdbc:mysql://{rdb_host}:{rdb_port}/{rdb_name}'
                config['root']['jdbc.url'] = dsn
                config['root']['jdbc.driver'] = 'com.mysql.cj.jdbc.Driver'
            elif rdb_type == 'PostgreSQL':
                dsn = f'jdbc:postgresql://{rdb_host}:{rdb_port}/{rdb_name}'
                config['root']['jdbc.url'] = dsn
                config['root']['jdbc.driver'] = 'org.postgresql.Driver'
            else:
                raise ValueError(f'Unknown RDB type: "{rdb_type}"')

            path = os.path.join(self._data_path, f'ontop{mode}')
            os.makedirs(path, exist_ok=True)
            with open(os.path.join(path, 'config.properties'), 'w') as f:
                config.write(f, space_around_delimiters=False)

            # .properties files are like .ini files but without a [HEADER]
            # Use a [root] header and remove it after writing
            with open(os.path.join(path, 'config.properties'), 'r') as f:
                data = f.read()

            with open(os.path.join(path, 'config.properties'), 'w') as f:
                f.write(data.replace('[root]\n', ''))
        else:
            raise ValueError('Ontop only supports RDBs')

        arguments.append('-m')
        arguments.append(os.path.join('/data/shared/', mapping_file))
        if output_file is not None:
            arguments.append('-o')
            arguments.append(os.path.join('/data/shared/', output_file))
        arguments.append('-p')
        arguments.append('/data/config.properties')

        return self.execute(mode, arguments)

class OntopVirtualize(_Ontop):
    def __init__(self, data_path: str, config_path: str, verbose: bool):
        self._data_path = os.path.abspath(data_path)
        self._config_path = os.path.abspath(config_path)
        os.makedirs(os.path.join(self._data_path, 'ontopendpoint'),
                    exist_ok=True)
        super().__init__('Ontop-Virtualize', data_path, verbose, 'endpoint')

    def execute_mapping(self, mapping_file: str, output_file: str = None,
                        serialization: str = None, rdb_username: str = None,
                        rdb_password: str = None, rdb_host: str = None,
                        rdb_port: str = None, rdb_name: str = None,
                        rdb_type: str = None) -> bool:
        config_file = f'{self._data_path}/ontopendpoint/config.properties'
        arguments = ['--cors-allowed-origins=*', '--port=8888']
        return super().execute_mapping('endpoint', config_file, arguments,
                                       mapping_file, output_file, rdb_username,
                                       rdb_password, rdb_host, rdb_port,
                                       rdb_name, rdb_type)

class OntopMaterialize(_Ontop):
    def __init__(self, data_path: str, config_path: str, verbose: bool):
        self._data_path = os.path.abspath(data_path)
        self._config_path = os.path.abspath(config_path)
        os.makedirs(os.path.join(self._data_path, 'ontopmaterialize'),
                    exist_ok=True)
        super().__init__('Ontop-Materialize', data_path, verbose, 'materialize')

    def execute_mapping(self, mapping_file: str, output_file: str,
                        serialization: str, rdb_username: str = None,
                        rdb_password: str = None, rdb_host: str = None,
                        rdb_port: str = None, rdb_name: str = None,
                        rdb_type: str = None) -> bool:
        config_file = f'{self._data_path}/ontopmaterialize/config.properties'
        arguments = [ '-f', serialization ]
        return super().execute_mapping('materialize', config_file, arguments,
                                       mapping_file, output_file, rdb_username,
                                       rdb_password, rdb_host, rdb_port,
                                       rdb_name, rdb_type)
