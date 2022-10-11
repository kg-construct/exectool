################################################################################
# RMLMapper
# https://github.com/RMLio/rmlmapper-java
################################################################################
FROM ubuntu:22.04
# Configure the RMLMapper version to use
ARG URL=https://github.com/RMLio/rmlmapper-java/releases/download/v6.0.0/rmlmapper-6.0.0-r363-all.jar

# Install latest updates and dependencies
RUN apt-get update
RUN apt-get upgrade -y
RUN apt-get install -y openjdk-8-jdk less vim wget unzip

# Download RMLMapper release
RUN mkdir rmlmapper && wget -O rmlmapper/rmlmapper.jar $URL

# Expose data folder
RUN mkdir /data

# Silent
CMD ["tail", "-f", "/dev/null"]
