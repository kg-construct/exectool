#!/usr/bin/env python3

import os
import tempfile
from container import Container

VERSION = '7.2.7'

class Virtuoso(Container):
    def __init__(self, data_path: str, config_path: str, verbose: bool):
        self._data_path = os.path.abspath(data_path)
        self._config_path = os.path.abspath(config_path)
        self._verbose = verbose
        tmp_dir = os.path.join(tempfile.gettempdir(), 'virtuoso')
        os.umask(0)
        os.makedirs(tmp_dir, exist_ok=True)
        os.makedirs(os.path.join(self._data_path, 'virtuoso'), exist_ok=True)
        super().__init__(f'dylanvanassche/virtuoso:v{VERSION}',
                         'Virtuoso', ports={'8890':'8890', '1111':'1111'},
                         environment={'DBA_PASSWORD':'root'},
                         volumes=[f'{self._data_path}/shared:/usr/share/proj',
                                  f'{tmp_dir}:/database'])
        self._endpoint = 'http://localhost:8890/sparql'

    def initialization(self) -> bool:
        # Virtuoso should start with a initialized database, start Virtuoso
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
        return self.run_and_wait_for_log('Server online at', command=command)

    def load(self, rdf_file: str) -> bool:
        success = True

        success, logs = self.exec(f'ls /usr/share/proj/{rdf_file}')
        self._logs += logs
        if not success:
            return False

        # Load directory with data
        success, logs = self.exec('isql -U dba -P root '
                                  'exec="ld_dir(\'/usr/share/proj/\','
                                  f'\'{rdf_file}\', '
                                  '\'http://example.com/graph\');"')
        self._logs += logs
        if not success:
            return False
        success, logs = self.exec('isql -U dba -P root '
                                  'exec="rdf_loader_run();"')
        self._logs += logs
        if not success:
            return False

        # Re-enable checkpoints and scheduler which are disabled automatically
        # after loading RDF with rdf_loader_run()
        success, logs = self.exec('isql -U dba -P root exec="checkpoint;"')
        self._logs += logs
        if not success:
            return False
        success, logs = self.exec('isql -U dba -P root '
                                  'exec="checkpoint_interval(60);"')
        self._logs += logs
        if not success:
            return False
        success, logs = self.exec('isql -U dba -P root '
                                  'exec="scheduler_interval(10);"')
        self._logs += logs
        if not success:
            return False

        return success

    def stop(self) -> bool:
        # Drop loaded triples
        success, logs = self.exec('isql -U dba -P root '
                                  'exec="delete from DB.DBA.load_list;"')
        self._logs += logs
        if not success:
            return False
        success, logs = self.exec('isql -U dba -P root '
                                  'exec="rdf_global_reset();"')
        self._logs += logs
        if not success:
            return False
        return super().stop()

    @property
    def endpoint(self):
        return self._endpoint

    @property
    def headers(self):
        return {}
