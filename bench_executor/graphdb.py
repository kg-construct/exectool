#!/usr/bin/env python3

from container import Container

class GraphDB(Container):
    def __init__(self):
        super().__init__()

    def root_mount_directory(self) -> str:
        return __name__.lower()

    def load(self):
        pass
