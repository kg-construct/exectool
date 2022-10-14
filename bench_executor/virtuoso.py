#!/usr/bin/env python3

import os
from container import Container

VERSION = '7.2.7'

class Virtuoso(Container):
    def __init__(self, data_path: str, verbose: bool):
        self._data_path = os.path.abspath(data_path)
        self._verbose = verbose
        super().__init__(f'openlink/virtuoso-opensource-7:{VERSION}', 'Virtuoso',
                         ports={'8890':'8890', '1111':'1111'},
                         environment={'DBA_PASSWORD':'root'},
                         volumes=[f'{data_path}/virtuoso:/database'])
        self._endpoint = 'http://localhost:8890/sparql'

    def wait_until_ready(self, command='') -> bool:
        return self.run_and_wait_for_log('Server online at', command=command)

    def load(self, rdf_file: str) -> bool:
        return True

    @property
    def endpoint(self):
        return self._endpoint
