#!/usr/bin/env python3

from container import Container

VERSION = '7.2.7'

class Virtuoso(Container):
    def __init__(self, data_path: str):
        super().__init__(f'openlink/virtuoso-opensource-7:{VERSION}', 'Virtuoso',
                         ports={'8890':'8890', '1111':'1111'},
                         environment={'DBA_PASSWORD':'root'},
                         volumes=[f'{data_path}/virtuoso:/database'])
        self._endpoint = 'http://localhost:8890/sparql'

    def wait_until_ready(self, command=''):
        self.run_and_wait_for_log('Server online at', command=command)

    def load(self):
        pass

    @property
    def endpoint(self):
        return self._endpoint
