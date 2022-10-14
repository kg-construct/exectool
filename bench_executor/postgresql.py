#!/usr/bin/env python3

import os
from container import Container

class PostgreSQL(Container):
    def __init__(self, data_path: str, verbose: bool):
        self._data_path = os.path.abspath(data_path)
        self._verbose = verbose

        super().__init__('postgres:14.5-bullseye', 'PostgreSQL',
                         ports={'5432': '5432'},
                         environment={'POSTGRES_PASSWORD': 'root',
                                      'POSTGRES_USER': 'root',
                                      'POSTGRES_DB': 'db',
                                      'POSTGRES_HOST_AUTH_METHOD': 'trust'},
                         volumes=[f'{self._data_path}/postgresql:/var/lib/postgresql/data'])

    def wait_until_ready(self, command: str = '') -> bool:
        return self.run_and_wait_for_log('port 5432', command=command)

    def load(self, csv_file: str = '', name: str = '') -> bool:
        return True
