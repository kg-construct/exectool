#!/usr/bin/env python3

import os
import sys
import requests
from typing import Optional, List

class Query():
    def __init__(self, data_path: str, config_path: str, verbose: bool):
        self._data_path = os.path.abspath(data_path)
        self._config_path = os.path.abspath(config_path)
        self._verbose = verbose
        self._logs = []

        os.makedirs(os.path.join(self._data_path, 'query'), exist_ok=True)

    def root_mount_directory(self) -> str:
        return __name__.lower()

    def execute(self, query, sparql_endpoint: str) -> str:
        self._logs.append(f'Executing query "{query}" on endpoint '
                          f'"{sparql_endpoint}"\n')
        data = {
            'query': query,
            'format': 'text/plain', # N-Triples Virtuoso
            'default-graph-uri': '', # Empty default graph Virtuoso
            'maxrows': 10000000 # Overwrite Virtuoso SPARQL limit
        }
        r = requests.post(sparql_endpoint, data=data)
        r.raise_for_status()
        return r.text

    def logs(self) -> Optional[List[str]]:
        return self._logs

    def execute_and_save(self, query: str, sparql_endpoint: str,
                         results_file: str) -> bool:
        try:
            results = self.execute(query, sparql_endpoint)
        except Exception as e:
            print(f'Failed to execute query "{query}" on endpoint '
                  f'"{sparql_endpoint}"', file=sys.stderr)
            self._logs.append(f'{e}\n')
            return False
        path = os.path.join(self._data_path, 'shared')
        os.makedirs(path, exist_ok=True)
        results_file = os.path.join(path, results_file)
        with open(results_file, 'w') as f:
            f.write(results)

        self._logs.append(f'Wrote query results to "{results_file}"\n')

        if self._verbose:
            self._logs.append('Query results:\n')
            self._logs.append(f'{results}\n')
            for line in self._logs:
                print(line)

        # Check results output
        if len(results) and 'Empty' not in results:
            return True
        else:
            self._logs.append(f'No results found!\n')
            print('No results found for query!', file=sys.stderr)
            return False
