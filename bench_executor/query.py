#!/usr/bin/env python3

import os
import sys
import requests
from typing import Optional, List
from timeout_decorator import timeout, TimeoutError
from logger import Logger

TIMEOUT = 1 * 3600 # 1 hour


class Query():
    def __init__(self, data_path: str, config_path: str, directory: str,
                 verbose: bool):
        self._data_path = os.path.abspath(data_path)
        self._config_path = os.path.abspath(config_path)
        self._logger = Logger(__name__, directory, verbose)
        self._logs = []

        os.umask(0)
        os.makedirs(os.path.join(self._data_path, 'query'), exist_ok=True)

    @property
    def name(self):
        return __name__

    @property
    def root_mount_directory(self) -> str:
        return __name__.lower()

    @timeout(TIMEOUT)
    def _execute_with_timeout(self, query: str, sparql_endpoint: str,
                              headers: dict = None) -> str:

        self._logger.info(f'Executing query "{query}" on endpoint '
                          f'"{sparql_endpoint}"')
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
            self._logger.error('Query failed: {r.text} (HTTP {r.status_code})')
        r.raise_for_status()
        return r.text

    def _execute(self, query: str, sparql_endpoint: str, expect_empty: bool,
                 headers: dict = None) -> Optional[str]:
        results = None

        try:
            results = self._execute_with_timeout(query, sparql_endpoint, headers)
        except TimeoutError:
            msg = f'Timeout ({TIMEOUT}s) reached for Query: "{query}"'
            self._logger.warning(msg)

        # Check results output
        if results is None or not results or 'Empty' in results:
            if expect_empty:
                self._logger.info('No results found, but was expected!')
                return None

            self._logger.error('No results found!')
            return None

        return results

    def logs(self) -> Optional[List[str]]:
        return self._logs

    def execute_and_save(self, query: str, sparql_endpoint: str,
                         results_file: str, headers: dict = None,
                         expect_empty: bool = False) -> bool:
        try:
            results = self._execute(query, sparql_endpoint, expect_empty,
                                    headers)
        except Exception as e:
            msg = f'Failed to execute query "{query}" on endpoint ' + \
                  f'"{sparql_endpoint}": {e}'
            self._logger.error(msg)
            return False

        path = os.path.join(self._data_path, 'shared')
        os.umask(0)
        os.makedirs(path, exist_ok=True)

        if results is not None:
            results_file = os.path.join(path, results_file)
            with open(results_file, 'w') as f:
                f.write(results)

            self._logger.debug(f'Wrote query results to "{results_file}"')
            self._logger.debug('Query results:')
            self._logger.debug(results)
            return True

        return False

    def _read_query_file(self, query_file: str) -> str:
        path = os.path.join(self._data_path, 'shared', query_file)
        if not os.path.exists(path):
            self._logger.error(f'Query file "{path}" does not exist')
            return False

        with open(path, 'r') as f:
            query = f.read()

        return query

    def execute_from_file(self, query_file: str, sparql_endpoint: str,
                          expect_empty: bool = False,
                          headers: dict = None) -> list:
        query = self._read_query_file(query_file)
        try:
            results = self._execute(query, sparql_endpoint, expect_empty,
                                    headers)
        except Exception as e:
            msg = f'Failed to execute query "{query}" on endpoint ' + \
                  f'"{sparql_endpoint}": {e}'
            self._logger.error(msg)
            return False

        if results is not None:
            return results

        return ''

    def execute_from_file_and_save(self, query_file: str,
                                   sparql_endpoint: str,
                                   results_file: str,
                                   expect_empty: bool = False,
                                   headers: dict = None) -> bool:
        query = self._read_query_file(query_file)
        results = self.execute_and_save(query, sparql_endpoint, results_file,
                                        expect_empty, headers)
        if results is not None:
            return True

        return False
