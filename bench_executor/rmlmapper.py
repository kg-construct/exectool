#!/usr/bin/env python3

from container import Container

VERSION = '6.0.0'

class RMLMapper(Container):
    def __init__(self, data_path):
        super().__init__(f'kg-construct/rmlmapper:v{VERSION}', 'RMLMapper',
                         volumes=[f'{data_path}/rmlmapper:/data'])

    def execute(self, arguments):
        # Example: [ '-m' 'mapping.rml.ttl' ]
        self.run(f'java -jar rmlmapper/rmlmapper.jar {" ".join(arguments)}')
        for line in self.logs():
            print(str(line.strip()))
