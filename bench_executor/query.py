#!/usr/bin/env python3

import os
import sys
import requests
from typing import Optional, List
from timeout_decorator import timeout, TimeoutError

TIMEOUT = 1 * 3600 # 1 hour

class Query():
    def __init__(self, data_path: str, config_path: str, verbose: bool):
        self._data_path = os.path.abspath(data_path)
        self._config_path = os.path.abspath(config_path)
        self._verbose = verbose
        self._logs = []
        self._name = 'Query'

        os.umask(0)
        os.makedirs(os.path.join(self._data_path, 'query'), exist_ok=True)

    @property
    def name(self):
        return self._name

    @property
    def root_mount_directory(self) -> str:
        return __name__.lower()

    @timeout(TIMEOUT)
    def _execute_with_timeout(self, query: str, sparql_endpoint: str,
                              headers: dict = None) -> str:

        self._logs.append(f'Executing query "{query}" on endpoint '
                          f'"{sparql_endpoint}"\n')
        data = {
            'query': query,
            'format': 'text/plain', # N-Triples Virtuoso
            'maxrows': '2000000' # Overwrite Virtuoso SPARQL limit
        }
        # Hardcoded to N-Triples
        if headers is not None:
            r = requests.post(sparql_endpoint, data=data, headers=headers)
        else:
            r = requests.post(sparql_endpoint, data=data)
        if r.status_code != 200:
            print(r.text, file=sys.stderr)
        r.raise_for_status()
        return r.text

    def execute(self, query: str, sparql_endpoint: str,
                headers: dict = None) -> Optional[str]:
        try:
            return self._execute_with_timeout(query, sparql_endpoint, headers)
        except TimeoutError:
            msg = f'Timeout reached for query: "{query}"'
            print(msg, file=sys.stderr)
            self._logs.append(msg)

        return None

    def logs(self) -> Optional[List[str]]:
        return self._logs

    def execute_and_save(self, query: str, sparql_endpoint: str,
                         results_file: str, headers: dict = None,
                         expect_empty: bool = False) -> bool:
        try:
            results = self.execute(query, sparql_endpoint, headers)
            if results is None:
                return False
        except Exception as e:
            print(f'Failed to execute query "{query}" on endpoint '
                  f'"{sparql_endpoint}": {e}', file=sys.stderr)
            self._logs.append(f'{e}\n')
            return False

        path = os.path.join(self._data_path, 'shared')
        os.umask(0)
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
        elif expect_empty:
            self._logs.append('No results found, but was expected!\n')
            print('No results found for query, but was expected!',
                  file=sys.stderr)
            return True
        else:
            self._logs.append('No results found!\n')
            print('No results found for query!', file=sys.stderr)
            return False

    def _read_query_file(self, query_file: str) -> str:
        path = os.path.join(self._data_path, 'shared', query_file)
        if not os.path.exists(path):
            msg = f'Query file "{path}" does not exist'
            print(msg, file=sys.stderr)
            self._logs.append(msg + '\n')
            return False

        with open(path, 'r') as f:
            query = f.read()

        return query

    def execute_from_file(self, query_file: str, sparql_endpoint: str,
                          headers: dict = None) -> bool:
        query = self._read_query_file(query_file)
        try:
            results = self.execute(query, sparql_endpoint, headers)
            if results is None:
                return False
        except Exception as e:
            print(f'Failed to execute query "{query}" on endpoint '
                  f'"{sparql_endpoint}": {e}', file=sys.stderr)
            self._logs.append(f'{e}\n')
            return False

        return results

    def execute_from_file_and_save(self, query_file: str,
                                   sparql_endpoint: str,
                                   results_file: str,
                                   headers: dict = None) -> bool:
        query = self._read_query_file(query_file)
        results = self.execute_and_save(query, sparql_endpoint, results_file,
                                        headers)
        return results
