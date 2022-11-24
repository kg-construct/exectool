#!/usr/bin/env python3

import os
import psutil
from container import Container

VERSION = '1.3.6'

class YARRRML(Container):
    def __init__(self, data_path: str, config_path: str, verbose: bool):
        self._data_path = os.path.abspath(data_path)
        self._config_path = os.path.abspath(config_path)
        os.makedirs(os.path.join(self._data_path, 'yarrrml'), exist_ok=True)
        super().__init__(f'blindreviewing/yarrrml:v{VERSION}', 'YARRRML',
                         verbose,
                         volumes=[f'{self._data_path}/yarrrml:/data',
                                  f'{self._data_path}/shared:/data/shared'])

    @property
    def root_mount_directory(self) -> str:
        return __name__.lower()

    def transform_mapping(self, yarrrml_file: str, mapping_file: str,
                          r2rml: bool = False, pretty: bool = True):
        arguments = ['-i', os.path.join('/data/shared/', yarrrml_file),
                     '-o', os.path.join('/data/shared/', mapping_file)]

        if r2rml:
            arguments.append('-c')
            arguments.append('-f R2RML')

        if pretty:
            arguments.append('-p')

        self._logs.append(f'Executing YARRRML with arguments '
                          f'"{" ".join(arguments)}"\n')

        cmd = f'{" ".join(arguments)}'
        success = self.run_and_wait_for_exit(cmd)
        return success
