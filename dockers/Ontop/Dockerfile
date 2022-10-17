################################################################################
# Ontop
# https://github.com/ontop/ontop
################################################################################
FROM ubuntu:22.04
# Configure the RMLMapper version to use
ARG URL=https://github.com/ontop/ontop/releases/download/ontop-4.2.0/ontop-cli-4.2.0.zip

# Install latest updates and dependencies
RUN apt-get update
RUN apt-get upgrade -y
RUN apt-get install -y openjdk-8-jdk less vim wget unzip

# Download Ontop release
RUN wget -O ontop.zip $URL
RUN unzip ontop.zip -d /ontop
COPY mysql-connector.jar /ontop/jdbc/
COPY postgresql-connector.jar /ontop/jdbc/

# Expose gtfs-bench folders
RUN mkdir /data
RUN mkdir /mappings
RUN mkdir /results
RUN mkdir /configs

# Silent
CMD ["tail", "-f", "/dev/null"]
