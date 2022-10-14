#!/usr/bin/env python3

import os
import requests

class Query():
    def __init__(self, data_path, verbose):
        self._data_path = os.path.abspath(data_path)
        self._verbose = verbose

    def execute(self, query, sparql_endpoint: str) -> str:
        data = {
            'query': query,
            'format': 'text/plain', # N-Triples Virtuoso
            'default-graph-uri': '' # Empty default graph Virtuoso
        }
        r = requests.post(sparql_endpoint, data=data)
        r.raise_for_status()
        return r.text

    def execute_and_save(self, query: str, sparql_endpoint: str,
                         results_file_name: str) -> bool:
        results = self.execute(query, sparql_endpoint)
        path = os.path.join(self._data_path, 'query')
        os.makedirs(path, exist_ok=True)
        with open(os.path.join(path, results_file_name), 'w') as f:
            f.write(results)

        if self._verbose:
            print('Query results:')
            print(results)

        # Check results output
        if len(results) and 'Empty' not in results:
            return True
        else:
            return False
