#!/usr/bin/env python3

import os
import tempfile
import psutil
from threading import Thread
from container import Container
from logger import Logger

VERSION = '7.2.7'
MAX_ROWS = '10000000'
QUERY_TIMEOUT = '0' # no limit
MAX_VECTOR_SIZE = '3000000' # max value is 'around' 3,500,000 from docs
PASSWORD = 'root'
NUMBER_OF_BUFFERS_PER_GB = 85000
MAX_DIRTY_BUFFERS_PER_GB = 65000

def _spawn_loader(container):
    success, logs = container.exec('isql -U dba -P root '
                                   'exec="rdf_loader_run();"')


class Virtuoso(Container):
    def __init__(self, data_path: str, config_path: str, directory: str,
                 verbose: bool):
        self._data_path = os.path.abspath(data_path)
        self._config_path = os.path.abspath(config_path)
        self._logger = Logger(__name__, directory, verbose)

        tmp_dir = os.path.join(tempfile.gettempdir(), 'virtuoso')
        os.umask(0)
        os.makedirs(tmp_dir, exist_ok=True)
        os.makedirs(os.path.join(self._data_path, 'virtuoso'), exist_ok=True)
        number_of_buffers = int(psutil.virtual_memory().total / (10**9) \
                                * NUMBER_OF_BUFFERS_PER_GB)
        max_dirty_buffers = int(psutil.virtual_memory().total / (10**9) \
                                * MAX_DIRTY_BUFFERS_PER_GB)
        environment={'DBA_PASSWORD': PASSWORD,
                     'VIRT_SPARQL_ResultSetMaxRows': MAX_ROWS,
                     'VIRT_SPARQL_MaxQueryExecutionTime': QUERY_TIMEOUT,
                     'VIRT_SPARQL_ExecutionTimeout': QUERY_TIMEOUT,
                     'VIRT_SPARQL_MaxQueryCostEstimationTime': QUERY_TIMEOUT,
                     'VIRT_Parameters_MaxVectorSize': MAX_VECTOR_SIZE,
                     'VIRT_Parameters_NumberOfBuffers': number_of_buffers,
                     'VIRT_Parameters_MaxDirtyBuffers': max_dirty_buffers}
        super().__init__(f'blindreviewing/virtuoso:v{VERSION}',
                         'Virtuoso', self._logger,
                         ports={'8890':'8890', '1111':'1111'},
                         environment=environment,
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
        return self.load_parallel(rdf_file, 1)

    def load_parallel(self, rdf_file: str, cores: int) -> bool:
        success = True

        success, logs = self.exec(f'sh -c "ls /usr/share/proj/{rdf_file}"')
        for line in logs:
            self._logger.debug(line)
        if not success:
            self._logger.error('RDF files do not exist for loading')
            return False

        # Load directory with data
        success, logs = self.exec('isql -U dba -P root '
                                  'exec="ld_dir(\'/usr/share/proj/\','
                                  f'\'{rdf_file}\', '
                                  '\'http://example.com/graph\');"')
        for line in logs:
            self._logger.debug(line)
        if not success:
            self._logger.error('ISQL loader query failure')
            return False

        loader_threads = []
        self._logger.debug(f'Spawning {cores} loader threads')
        for i in range(cores):
            t = Thread(target=_spawn_loader, args=(self,), daemon=True)
            t.start()
            loader_threads.append(t)

        for t in loader_threads:
            t.join()
        self._logger.debug(f'Loading finished with {cores} threads')

        # Re-enable checkpoints and scheduler which are disabled automatically
        # after loading RDF with rdf_loader_run()
        success, logs = self.exec('isql -U dba -P root exec="checkpoint;"')
        for line in logs:
            self._logger.debug(line)
        if not success:
            self._logger.error('ISQL re-enable checkpoints query failure')
            return False

        success, logs = self.exec('isql -U dba -P root '
                                  'exec="checkpoint_interval(60);"')
        for line in logs:
            self._logger.debug(line)
        if not success:
            self._logger.error('ISQL checkpoint interval query failure')
            return False

        success, logs = self.exec('isql -U dba -P root '
                                  'exec="scheduler_interval(10);"')
        for line in logs:
            self._logger.debug(line)
        if not success:
            self._logger.error('ISQL scheduler interval query failure')
            return False

        return success

    def stop(self) -> bool:
        # Drop loaded triples
        success, logs = self.exec('isql -U dba -P root '
                                  'exec="delete from DB.DBA.load_list;"')
        for line in logs:
            self._logger.debug(line)
        if not success:
            self._logger.error('ISQL delete load list query failure')
            return False

        success, logs = self.exec('isql -U dba -P root '
                                  'exec="rdf_global_reset();"')
        for line in logs:
            self._logger.debug(line)
        if not success:
            self._logger.error('ISQL RDF global reset query failure')
            return False
        return super().stop()

    @property
    def endpoint(self):
        return self._endpoint

    @property
    def headers(self):
        return {}

if __name__ == '__main__':
    print('ℹ️  Starting up...')
    v = Virtuoso('data', 'config', True)
    v.wait_until_ready()
    input('ℹ️  Press any key to stop')
    v.stop()
    print('ℹ️  Stopped')
