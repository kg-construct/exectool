#!/usr/bin/env python3

from docker import Docker

class MySQL(Docker):
    def __init__(self):
        super().__init__()

    def load(self):
        pass
