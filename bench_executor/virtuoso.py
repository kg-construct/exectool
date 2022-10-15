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
                         volumes=[f'{self._data_path}/virtuoso/virtuoso.ini:/database/virtuoso.ini',
                                  f'{self._data_path}/virtuoso/data:/database',
                                  f'{self._data_path}/shared:/data/shared'])
        self._endpoint = 'http://localhost:8890/sparql'

    def wait_until_ready(self, command: str = '') -> bool:
        return self.run_and_wait_for_log('Server online at', command=command)

    def load(self, rdf_file: str) -> bool:
        success = True

        # Load directory with data
        success, logs = self.exec('isql -U dba -P root exec="ld_dir(\'/data/shared/\','
                                  f'\'{rdf_file}\', \'http://example.com/graph\');"')
        self._logs += logs
        success, logs = self.exec('isql -U dba -P root exec="rdf_loader_run();"')
        self._logs += logs

        # Re-enable checkpoints and scheduler which are disabled automatically
        # after loading RDF with rdf_loader_run()
        success, logs = self.exec('isql -U dba -P root exec="checkpoint;"')
        self._logs += logs
        success, logs = self.exec('isql -U dba -P root exec="checkpoint_interval(60);"')
        self._logs += logs
        success, logs = self.exec('isql -U dba -P root exec="scheduler_interval(10);"')
        self._logs += logs

        return success

    @property
    def endpoint(self):
        return self._endpoint
