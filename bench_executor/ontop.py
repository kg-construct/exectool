#!/usr/bin/env python3

import os
import sys
import psutil
import configparser
from container import Container
from rdflib import Graph, Namespace, RDF, URIRef

VERSION = '4.2.1-PATCH' # 4.2.1 with N-Triples and N-Quads support
R2RML = Namespace('http://www.w3.org/ns/r2rml#')

class _Ontop(Container):
    def __init__(self, name: str, data_path: str, verbose: bool, mode: str):
        self._mode = mode
        self._headers = {}

        os.makedirs(os.path.join(self._data_path, f'ontop{self._mode}'),
                    exist_ok=True)

        # Set Java heap to 1/2 of available memory instead of the default 1/4
        max_heap = int(psutil.virtual_memory().total * (1/2))

        environment = {'ONTOP_JAVA_ARGS': f'-Xmx{max_heap} -Xms{max_heap}'}
        super().__init__(f'blindreviewing/ontop:v{VERSION}', name,
                         verbose,
                         ports={'8888':'8888'},
                         environment=environment,
                         volumes=[f'{self._data_path}/'
                                  f'{self.root_mount_directory}:/data',
                                  f'{self._data_path}/shared:/data/shared'])

    @property
    def root_mount_directory(self) -> str:
        if self._mode == 'endpoint':
            return 'ontopvirtualize'
        elif self._mode == 'materialize':
            return 'ontopmaterialize'
        else:
            raise ValueError(f'Unknown Ontop mode: "{self._mode}"')

    @property
    def endpoint(self) -> str:
        return 'http://localhost:8888/sparql'

    @property
    def headers(self) -> dict:
        return self._headers

    def execute(self, arguments: list) -> bool:
        cmd = f'/ontop/ontop {self._mode} {" ".join(arguments)}'
        if self._mode == 'endpoint':
            log_line = 'OntopEndpointApplication - Started ' + \
                       'OntopEndpointApplication'
            success = self.run_and_wait_for_log(log_line, cmd)
        elif self._mode == 'materialize':
            success = self.run_and_wait_for_exit(cmd)
        else:
            print(f'Unknown Ontop mode "{self._mode}"', file=sys.stderr)
            success = False

        return success

    def execute_mapping(self, config_file: str, arguments: list,
                        mapping_file: str, output_file: str,
                        rdb_username: str = None, rdb_password: str = None,
                        rdb_host: str = None, rdb_port: str = None,
                        rdb_name: str = None, rdb_type: str = None) -> bool:
        # Generate INI configuration file since no CLI is available
        if rdb_username is not None and rdb_password is not None \
            and rdb_host is not None and rdb_port is not None \
            and rdb_name is not None and rdb_type is not None:
            config = configparser.ConfigParser()
            config['root'] = {
                'jdbc.user': rdb_username,
                'jdbc.password': rdb_password
            }
            if rdb_type == 'MySQL':
                dsn = f'jdbc:mysql://{rdb_host}:{rdb_port}/{rdb_name}'
                config['root']['jdbc.url'] = dsn
                config['root']['jdbc.driver'] = 'com.mysql.cj.jdbc.Driver'
            elif rdb_type == 'PostgreSQL':
                dsn = f'jdbc:postgresql://{rdb_host}:{rdb_port}/{rdb_name}'
                config['root']['jdbc.url'] = dsn
                config['root']['jdbc.driver'] = 'org.postgresql.Driver'
            else:
                raise ValueError(f'Unknown RDB type: "{rdb_type}"')

            path = os.path.join(self._data_path, self.root_mount_directory)
            os.makedirs(path, exist_ok=True)
            with open(os.path.join(path, 'config.properties'), 'w') as f:
                config.write(f, space_around_delimiters=False)

            # .properties files are like .ini files but without a [HEADER]
            # Use a [root] header and remove it after writing
            with open(os.path.join(path, 'config.properties'), 'r') as f:
                data = f.read()

            with open(os.path.join(path, 'config.properties'), 'w') as f:
                f.write(data.replace('[root]\n', ''))
        else:
            raise ValueError('Ontop only supports RDBs')

        # Compatibility with Ontop requiring rr:class
        # Replace any rdf:type construction with rr:class
        # Without this, a strange error is raised: 'The definition of the
        # predicate is not always a ground term triple(s,p,o)'
        g = Graph()
        g.bind('r2rml', R2RML)
        g.bind('rdf', RDF)
        g.parse(os.path.join(self._data_path, 'shared',
                             os.path.basename(mapping_file)))

        for triples_map_iri, p, o in g.triples((None, RDF.type,
                                                R2RML.TriplesMap)):
            subject_map_iri = g.value(triples_map_iri, R2RML.subjectMap)

            for s, p, predicate_object_map_iri in g.triples((triples_map_iri, R2RML.predicateObjectMap, None)):
                predicate_map_iri = g.value(predicate_object_map_iri,
                                            R2RML.predicateMap)
                object_map_iri = g.value(predicate_object_map_iri,
                                         R2RML.objectMap)

                if predicate_map_iri is None or object_map_iri is None:
                    continue

                # Check if PredicateObjectMap is pointing to a PredicateMap
                # specifying rdf:type. Skip this PredicateObjectMap if not
                if g.value(predicate_map_iri, R2RML.constant) != RDF.type:
                    continue

                # Retrieve the ObjectMap rr:constant value and add it as
                # rr:class to the Subject Map is present
                rdf_type_value = g.value(object_map_iri, R2RML.constant)
                if rdf_type_value is not None:
                    iri = URIRef(rdf_type_value.toPython())
                    g.add((subject_map_iri, R2RML['class'], iri))
                else:
                    print('Cannot extract rr:class value, rdf:type value is not'
                          ' a constant value!', file=sys.stderr)
                    return False

                # Remove all triples associated with the rdf:type PredicateMap
                for s, p, o in g.triples((predicate_map_iri, None, None)):
                    g.remove((s, p, o))

                # Remove all triples associated with the rdf:type ObjectMap
                for s, p, o in g.triples((object_map_iri, None, None)):
                    g.remove((s, p, o))

                # Remove all triples associated with the
                # rdf:type PredicateObjectMap
                for s, p, o in g.triples((object_map_iri, None, None)):
                    g.remove((s, p, o))

                # Remove PredicateObjectMap from Triples Map
                g.remove((triples_map_iri, R2RML.predicateObjectMap,
                          predicate_object_map_iri))

            destination = os.path.join(self._data_path,
                                       self.root_mount_directory,
                                       'mapping_converted.r2rml.ttl')
            g.serialize(destination=destination, format='turtle')

        arguments.append('-m')
        arguments.append('/data/mapping_converted.r2rml.ttl')
        if output_file is not None:
            arguments.append('-o')
            arguments.append(os.path.join('/data/shared/', output_file))
        arguments.append('-p')
        arguments.append('/data/config.properties')

        return self.execute(arguments)

class OntopVirtualize(_Ontop):
    def __init__(self, data_path: str, config_path: str, verbose: bool):
        self._data_path = os.path.abspath(data_path)
        self._config_path = os.path.abspath(config_path)
        super().__init__('Ontop-Virtualize', data_path, verbose, 'endpoint')

    def execute_mapping(self, mapping_file: str, output_file: str = None,
                        serialization: str = "ntriples",
                        rdb_username: str = None, rdb_password: str = None,
                        rdb_host: str = None, rdb_port: str = None,
                        rdb_name: str = None, rdb_type: str = None) -> bool:
        config_file = f'{self._data_path}/{self.root_mount_directory}' + \
                      '/config.properties'
        arguments = ['--cors-allowed-origins=*', '--port=8888']
        if serialization == 'ntriples':
            self._headers = { 'Accept': 'application/n-triples' }
        elif serialization == 'nquads':
            self._headers = { 'Accept': 'application/n-quads' }
        elif serialization == 'turtle':
            self._headers = { 'Accept': 'text/turtle' }
        elif serialization == 'rdfjson':
            self._headers = { 'Accept': 'application/rdf+json' }
        elif serialization == 'rdfxml':
            self._headers = { 'Accept': 'application/rdf+xml' }
        elif serialization == 'jsonld':
            self._headers = { 'Accept': 'application/ld+json' }
        elif serialization == 'csv':
            self._headers = { 'Accept': 'text/csv' }
        else:
            raise ValueError(f'Unsupported serialization format '
                             f'"{serialization}" for Ontop')
        return super().execute_mapping(config_file, arguments,
                                       mapping_file, output_file, rdb_username,
                                       rdb_password, rdb_host, rdb_port,
                                       rdb_name, rdb_type)

class OntopMaterialize(_Ontop):
    def __init__(self, data_path: str, config_path: str, verbose: bool):
        self._data_path = os.path.abspath(data_path)
        self._config_path = os.path.abspath(config_path)
        os.makedirs(os.path.join(self._data_path, 'ontopmaterialize'),
                    exist_ok=True)
        super().__init__('Ontop-Materialize', data_path, verbose, 'materialize')

    def execute_mapping(self, mapping_file: str, output_file: str,
                        serialization: str, rdb_username: str = None,
                        rdb_password: str = None, rdb_host: str = None,
                        rdb_port: str = None, rdb_name: str = None,
                        rdb_type: str = None) -> bool:
        config_file = f'{self._data_path}/{self.root_mount_directory}' + \
                      '/config.properties'
        arguments = [ '-f', serialization ]
        self._headers = { }
        return super().execute_mapping(config_file, arguments,
                                       mapping_file, output_file, rdb_username,
                                       rdb_password, rdb_host, rdb_port,
                                       rdb_name, rdb_type)
