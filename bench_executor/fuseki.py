#!/usr/bin/env python3

import os
import sys
import requests
import tempfile
import psutil
from container import Container

VERSION = '4.6.1'
CMD_ARGS = '--tdb2 --update --loc /fuseki/databases/DB /ds'

class Fuseki(Container):
    def __init__(self, data_path: str, config_path: str, verbose: bool):
        self._data_path = os.path.abspath(data_path)
        self._config_path = os.path.abspath(config_path)
        tmp_dir = os.path.join(tempfile.gettempdir(), 'fuseki')
        os.umask(0)
        os.makedirs(tmp_dir, exist_ok=True)
        os.makedirs(os.path.join(self._data_path, 'fuseki'), exist_ok=True)

        # Set Java heap to 1/2 of available memory instead of the default 1/4
        max_heap = int(psutil.virtual_memory().total * (1/2))

        super().__init__(f'blindreviewing/fuseki:v{VERSION}', 'Fuseki',
                         verbose,
                         ports={'3030':'3030'},
                         environment={
                             'JAVA_OPTIONS':f'-Xmx{max_heap} -Xms{max_heap}'
                         },
                         volumes=[f'{self._config_path}/fuseki/'
                                  f'log4j2.properties:/fuseki/'
                                  f'log4j2.properties',
                                  f'{self._data_path}/shared:/data',
                                  f'{tmp_dir}:/fuseki/databases/DB'])
        self._endpoint = 'http://localhost:3030/ds/sparql'

    def initialization(self) -> bool:
        # Fuseki should start with a initialized database, start Fuseki
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

    @property
    def headers(self) -> str:
        return {}

    def wait_until_ready(self, command: str = '') -> bool:
        command = f'{command} {CMD_ARGS}'
        return self.run_and_wait_for_log(':: Start Fuseki ', command=command)

    def load(self, rdf_file: str) -> bool:
        path = os.path.join(self._data_path, 'shared', rdf_file)

        if not os.path.exists(path):
            print(f'RDF file "{rdf_file}" does not exist', file=sys.stderr)
            return False

        # Load directory with data with HTTP post
        try:
            r = requests.post('http://localhost:3030/ds', data=open(path, 'rb'),
                              headers={'Content-Type': 'application/n-triples'})
            self._logs.append(f'Loading triples: {r.text}\n')
            r.raise_for_status()
        except Exception as e:
            print(f'Failed to load RDF: "{e}" into Fuseki', file=sys.stderr)
            return False

        return True

    def stop(self) -> bool:
        # Drop triples on exit
        try:
            headers = {'Content-Type': 'application/sparql-update'}
            data = 'DELETE { ?s ?p ?o . } WHERE { ?s ?p ?o . }'
            r = requests.post('http://localhost:3030/ds/update',
                              headers=headers, data=data)
            self._logs.append(f'Dropping triples: {r.text}\n')
            r.raise_for_status()
        except Exception as e:
            print(f'Failed to drop RDF: "{e}" from Fuseki', file=sys.stderr)
            return False

        return super().stop()

    @property
    def endpoint(self):
        return self._endpoint

if __name__ == '__main__':
    print('ℹ️  Starting up...')
    f = Fuseki('data', 'config', True)
    f.wait_until_ready()
    input('ℹ️  Press any key to stop')
    f.stop()
    print('ℹ️  Stopped')
