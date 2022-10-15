#!/usr/bin/env python3

import os
import sys
import requests
from typing import Optional, List

class Query():
    def __init__(self, data_path, verbose):
        self._data_path = os.path.abspath(data_path)
        self._verbose = verbose
        self._logs = []

    def root_mount_directory(self) -> str:
        return __name__.lower()

    def execute(self, query, sparql_endpoint: str) -> str:
        self._logs.append(f'Executing query "{query}" on endpoint '
                          f'"{sparql_endpoint}"\n')
        data = {
            'query': query,
            'format': 'text/plain', # N-Triples Virtuoso
            'default-graph-uri': '' # Empty default graph Virtuoso
        }
        r = requests.post(sparql_endpoint, data=data)
        r.raise_for_status()
        return r.text

    def logs(self) -> Optional[List[str]]:
        return self._logs

    def execute_and_save(self, query: str, sparql_endpoint: str,
                         results_file_name: str) -> bool:
        results = self.execute(query, sparql_endpoint)
        path = os.path.join(self._data_path, 'query')
        os.makedirs(path, exist_ok=True)
        results_file = os.path.join(path, results_file_name)
        with open(results_file, 'w') as f:
            f.write(results)

        self._logs.append(f'Wrote query results to "{results_file}"\n')

        if self._verbose:
            print('Query results:')
            print(results)

        # Check results output
        if len(results) and 'Empty' not in results:
            return True
        else:
            self._logs.append(f'No results found!\n')
            print('No results found for query!', file=sys.stderr)
            return False
