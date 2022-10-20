#!/usr/bin/env python3

import os
import sys
import pymysql
import tempfile
from csv import reader
from container import Container

VERSION = '8.0'
HOST = 'localhost'
USER = 'root'
PASSWORD = 'root'
DB = 'db'

class MySQL(Container):
    def __init__(self, data_path: str, config_path: str, verbose: bool):
        self._data_path = os.path.abspath(data_path)
        self._config_path = os.path.abspath(config_path)
        self._verbose = verbose
        self._tables = []
        tmp_dir = os.path.join(tempfile.gettempdir(), 'mysql')
        os.makedirs(tmp_dir, exist_ok=True)
        os.makedirs(os.path.join(self._data_path, 'mysql'), exist_ok=True)

        super().__init__(f'mysql:{VERSION}-debian', 'MySQL',
                         ports={'3306':'3306'},
                         environment={'MYSQL_ROOT_PASSWORD': 'root',
                                      'MYSQL_DATABASE': 'db'},
                         volumes=[f'{self._data_path}/shared/:/data/shared',
                                  f'{self._config_path}/mysql/mysql-secure-file-prive.cnf:'
                                  '/etc/mysql/conf.d/mysql-secure-file-prive.cnf',
                                  f'{tmp_dir}:/var/lib/mysql'])

    def initialization(self) -> bool:
        # MySQL should start with a initialized database, start MySQL
        # if not initialized to avoid the pre-run start during benchmark
        # execution
        success = self.wait_until_ready()
        if not success:
            return False
        success = self.stop()

        return success

    def root_mount_directory(self) -> str:
        return __name__.lower()

    def wait_until_ready(self, command: str = '') -> bool:
        return self.run_and_wait_for_log('port: 3306  MySQL Community Server - GPL.', command=command)

    def load(self, csv_file: str = '', table: str = '') -> bool:
        success = True
        columns = None
        table = table.lower()
        path = os.path.join(self._data_path, 'shared', csv_file)

        self._tables.append(table)

        # Analyze and move CSV for loading
        if not os.path.exists(path):
            print(f'CSV file "{path}" does not exist', file=sys.stderr)
            return False

        with open(path, 'r') as f:
            csv_reader = reader(f)
            columns = next(csv_reader)
            columns = [x.lower() for x in columns]

        # Load CSV
        connection = pymysql.connect(host=HOST, user=USER, password=PASSWORD,
                                     db=DB)
        try:
            cursor = connection.cursor()

            cursor.execute(f'DROP TABLE IF EXISTS {table};')
            c = ' TEXT , '.join(columns) + ' TEXT'
            cursor.execute(f'CREATE TABLE {table} (k INT ZEROFILL NOT NULL '
                           f'AUTO_INCREMENT, {c}, PRIMARY KEY(k));')
            c = ','.join(columns)
            cursor.execute(f'LOAD DATA INFILE \'/data/shared/{csv_file}\' '
                           f'INTO TABLE {table} FIELDS TERMINATED BY \',\' '
                           f'ENCLOSED BY \'\\"\' LINES TERMINATED BY \'\\n\' '
                           f'IGNORE 1 ROWS ({c});')
            cursor.execute('COMMIT;')

            if self._verbose:
                header = '| ID | ' + ' | '.join(columns) + ' |'
                print(header)
                print('-' * len(header))

            cursor.execute(f'SELECT * FROM {table};')
            number_of_records = 0
            for record in cursor:
                number_of_records += 1
                if self._verbose:
                    print(record)

            if number_of_records == 0:
                success = False
        except Exception as e:
            print(f'Failed to load CSV: "{e}"', file=sys.stderr)
            success = False
        finally:
            connection.close()

        return success

    def stop(self) -> bool:
        connection = pymysql.connect(host=HOST, database=DB,
                                     user=PASSWORD, password=PASSWORD)
        cursor = connection.cursor()
        for table in self._tables:
            cursor.execute(f'DROP TABLE IF EXISTS {table};')
            cursor.execute(f'COMMIT;')
        self._tables = []
        connection.close()

        return super().stop()
