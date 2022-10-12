#!/usr/bin/env python3

from container import Container

class PostgreSQL(Container):
    def __init__(self, data_path: str):
        super().__init__('postgres:14.5-bullseye', 'PostgreSQL',
                         ports={'5432': '5432'},
                         environment={'POSTGRES_PASSWORD': 'root',
                                      'POSTGRES_USER': 'root',
                                      'POSTGRES_DB': 'db',
                                      'POSTGRES_HOST_AUTH_METHOD': 'trust'},
                         volumes=[f'{data_path}/postgresql:/var/lib/postgresql/data'])

    def wait_until_ready(self, command=''):
        self.run_and_wait_for_log('port 5432', command=command)

    def load(self):
        pass
