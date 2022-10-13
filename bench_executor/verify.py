#!/usr/bin/env python3

class _Check:
    def __init__(self):
        pass

class ResultsCheck(_Check):
    def __init__(self):
        super().__init__()

    def ok(self) -> bool:
        return True

class MetricsCheck(_Check):
    def __init__(self):
        super().__init__()

    def ok(self) -> bool:
        return True
