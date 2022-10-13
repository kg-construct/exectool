#!/usr/bin/env python3

from container import Container

VERSION = '8.0'

class MySQL(Container):
    def __init__(self, data_path: str):
        super().__init__(f'mysql:{VERSION}-debian', 'MySQL',
                         ports={'3306':'3306'},
                         environment={'MYSQL_ROOT_PASSWORD': 'root',
                                      'MYSQL_DATABASE': 'db'},
                         volumes=[f'{data_path}/mysql/data:/var/lib/mysql',
                                  f'{data_path}/mysql/:/data',
                                  f'{data_path}/mysql/mysql-secure-file-prive.cnf:'
                                  '/etc/mysql/conf.d/mysql-secure-file-prive.cnf'])

    def wait_until_ready(self, command=''):
        self.run_and_wait_for_log('port: 3306  MySQL Community Server - GPL.', command=command)

    def load(self, csv_file, name):
        pass
