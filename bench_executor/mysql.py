#!/usr/bin/env python3

import os
import sys
import pymysql
import tempfile
from csv import reader
from container import Container
from typing import List
from timeout_decorator import timeout, TimeoutError

VERSION = '8.0'
HOST = 'localhost'
USER = 'root'
PASSWORD = 'root'
DB = 'db'
PORT = '3306'
CLEAR_TABLES_TIMEOUT = 5 * 60 # 5 minutes

class MySQL(Container):
    def __init__(self, data_path: str, config_path: str, verbose: bool):
        self._data_path = os.path.abspath(data_path)
        self._config_path = os.path.abspath(config_path)
        self._tables = []
        tmp_dir = os.path.join(tempfile.gettempdir(), 'mysql')
        os.umask(0)
        os.makedirs(tmp_dir, exist_ok=True)
        os.makedirs(os.path.join(self._data_path, 'mysql'), exist_ok=True)

        super().__init__(f'blindreviewing/mysql:v{VERSION}', 'MySQL',
                         verbose,
                         ports={PORT:PORT},
                         environment={'MYSQL_ROOT_PASSWORD': 'root',
                                      'MYSQL_DATABASE': 'db'},
                         volumes=[f'{self._data_path}/shared/:/data/shared',
                                  f'{self._config_path}/mysql/'
                                  f'mysql-secure-file-prive.cnf:'
                                  f'/etc/mysql/conf.d/'
                                  f'mysql-secure-file-prive.cnf',
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

    @property
    def root_mount_directory(self) -> str:
        return __name__.lower()

    def wait_until_ready(self, command: str = '') -> bool:
        log_line = f'port: {PORT}  MySQL Community Server - GPL.'
        return self.run_and_wait_for_log(log_line, command=command)

    def load(self, csv_file: str, table: str) -> bool:
        return self._load_csv(csv_file, table, True)

    def load_multiple(self, csv_files: List[dict]) -> bool:
        for entry in csv_files:
            if not self._load_csv(entry['file'], entry['table'], True):
                return False
        return True

    def load_sql_schema(self, schema_file: str, csv_files: List[str]) -> bool:
        success = True

        # Load SQL schema
        cmd = f'/bin/sh -c \'mysql --host={HOST} --port={PORT} --user={USER} ' + \
              f'--password={PASSWORD} --database={DB} ' + \
              f'< /data/shared/{schema_file}\''
        success, output = self.exec(cmd)

        # Load CSVs
        if success:
            for csv_file, table in csv_files:
                success = self._load_csv(csv_file, table, False)
                if not success:
                    break

        return success

    def _load_csv(self, csv_file: str, table: str, create: bool) -> bool:
        success = True
        columns = None
        table = table.lower()
        path = os.path.join(self._data_path, 'shared', csv_file)
        path2 = os.path.join(self._data_path, 'shared', f'tmp_{csv_file}')

        self._tables.append(table)

        # Analyze CSV for loading
        if not os.path.exists(path):
            print(f'CSV file "{path}" does not exist', file=sys.stderr)
            return False

        with open(path, 'r') as f:
            csv_reader = reader(f)
            columns = next(csv_reader)
            columns = [x.lower() for x in columns]

        # MySQL cannot set NULL as NULL keyword, use their own specific syntax
        # for this: \N
        with open(path, 'r') as f:
            data = f.read()
            data = data.replace('NULL', '\\N')

            with open(path2, 'w') as f2:
                f2.write(data)

        # Load CSV
        connection = pymysql.connect(host=HOST, user=USER, password=PASSWORD,
                                     db=DB)
        try:
            cursor = connection.cursor()

            if create:
                cursor.execute(f'DROP TABLE IF EXISTS {table};')
                c = ' TEXT , '.join(columns) + ' TEXT'
                cursor.execute(f'CREATE TABLE {table} (k INT ZEROFILL NOT NULL '
                               f'AUTO_INCREMENT, {c}, PRIMARY KEY(k));')
            c = ','.join(columns)
            cursor.execute(f'LOAD DATA INFILE \'/data/shared/tmp_{csv_file}\' '
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

    @timeout(CLEAR_TABLES_TIMEOUT)
    def _clear_tables(self):
        connection = pymysql.connect(host=HOST, database=DB,
                                     user=PASSWORD, password=PASSWORD)
        cursor = connection.cursor()
        for table in self._tables:
            cursor.execute(f'DROP TABLE IF EXISTS {table};')
            cursor.execute(f'COMMIT;')
        self._tables = []
        connection.close()

    def stop(self) -> bool:
        try:
            self._clear_tables()
        except TimeoutError:
            print('Clearing MySQL tables timed out after '
                  f'{CLEAR_TABLES_TIMEOUT}s!', file=sys.stderr)

        return super().stop()
