#!/usr/bin/env python3

import os
import sys
import psycopg2
from psycopg2 import sql
from csv import reader
from time import sleep
from container import Container

HOST = 'localhost'
USER = 'root'
PASSWORD = 'root'
DB = 'db'
WAIT_TIME = 1

class PostgreSQL(Container):
    def __init__(self, data_path: str, verbose: bool):
        self._data_path = os.path.abspath(data_path)
        self._verbose = verbose

        super().__init__('postgres:14.5-bullseye', 'PostgreSQL',
                         ports={'5432': '5432'},
                         environment={'POSTGRES_PASSWORD': PASSWORD,
                                      'POSTGRES_USER': USER,
                                      'POSTGRES_DB': DB,
                                      'POSTGRES_HOST_AUTH_METHOD': 'trust'},
                         volumes=[f'{self._data_path}/postgresql:/var/lib/postgresql/data',
                                  f'{self._data_path}/shared:/data/shared'])

    def wait_until_ready(self, command: str = '') -> bool:
        success = self.run_and_wait_for_log('port 5432', command=command)
        while not self.exec('pg_isready -q'):
            print(f'PostgreSQL is not online yet... Trying again in 1s')
            sleep(WAIT_TIME)

        return success

    def load(self, csv_file_name: str, name: str) -> bool:
        success = True
        columns = None
        name = name.lower()
        path = os.path.join(self._data_path, 'shared', csv_file_name)

        # Analyze and move CSV for loading
        if not os.path.exists(path):
            print(f'CSV file "{path}" does not exist', file=sys.stderr)
            return False

        with open(path, 'r') as f:
            csv_reader = reader(f)
            columns = next(csv_reader)
            columns = [x.lower() for x in columns]

        # Load CSV
        connection = psycopg2.connect(host=HOST, database=DB,
                                      user=PASSWORD, password=PASSWORD)
        try:
            cursor = connection.cursor()

            cursor.execute(f'DROP TABLE IF EXISTS {name};')
            c = 'VARCHAR ,'.join(columns) + ' VARCHAR'
            cursor.execute(f'CREATE TABLE {name} (KEY SERIAL, {c}, '
                           'PRIMARY KEY(KEY))')
            c = ','.join(columns)
            cursor.execute(f'COPY {name} ({c}) FROM '
                           f'\'/data/shared/{csv_file_name}\' '
                           'DELIMITER \',\' NULL \'NULL\' CSV HEADER;')
            cursor.execute('COMMIT;')

            if self._verbose:
                cursor.execute(f'SELECT * FROM {name};')
                for record in cursor:
                    print(record)
        except Exception as e:
            print(f'Failed to load CSV: "{e}"')
            success = False
        finally:
            connection.close()

        return success
