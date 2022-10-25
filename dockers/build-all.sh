#!/bin/sh

# Fuseki 4.6.1
echo "Build Fuseki 4.6.1 ..."
cd Fuseki
docker build -t apache/fuseki:v4.6.1 .
cd ..

# Morph-KGC 2.2.0
echo "Build Morph-KGC 2.2.0 ..."
cd Morph-KGC
docker build -t kg-construct/morph-kgc:v2.2.0 .
cd ..

# Morph-RDB 3.12.5
echo "Building Morph-RDB 3.12.5 ..."
cd Morph-RDB
docker build -t kg-construct/morph-rdb:v3.12.5 .

# Ontop 4.2.0-PATCH
echo "Building Ontop 4.2.0-PATCH ..."
cd Ontop
docker build -f "Dockerfile.source" -t kg-construct/ontop:v4.2.1-PATCH .
cd ..

# RMLMapper 6.0.0
echo "Building RMLMapper 6.0.0 ..."
cd RMLMapper
docker build -t kg-construct/rmlmapper:v6.0.0 .
cd ..

# SDM-RDFizer 4.6
echo "Building SDM-RDFizer 4.6 ..."
cd SDM-RDFizer
docker build -t kg-construct/sdmrdfizer:v4.6 .
cd ..
