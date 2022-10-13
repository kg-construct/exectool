#!/usr/bin/env python3

import requests

class Query():
    def __init__(self, query: str):
        self._query = query

    def execute(self, sparql_endpoint: str) -> str:
        data = {
            'query': self._query,
            'format': 'text/plain', # N-Triples Virtuoso
            'default-graph-uri': '' # Empty default graph Virtuoso
        }
        r = requests.post(sparql_endpoint, data=data)
        r.raise_for_status()
        return r.text

    def execute_and_save(self, sparql_endpoint: str, results_file: str):
        results = self.execute(sparql_endpoint)
        with open(results_file, 'w') as f:
            f.write(results)
