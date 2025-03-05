# This image will be published as dspace/dspace
# See https://github.com/DSpace/DSpace/tree/main/dspace/src/main/docker for usage details
#
# - note: default tag for branch: dspace/dspace: dspace/dspace:latest

# This Dockerfile uses JDK17 by default.
# To build with other versions, use "--build-arg JDK_VERSION=[value]"
ARG JDK_VERSION=17
# The Docker version tag to build from
ARG DSPACE_VERSION=latest
# The Docker registry to use for DSpace images. Defaults to "docker.io"
# NOTE: non-DSpace images are hardcoded to use "docker.io" and are not impacted by this build argument
ARG DOCKER_REGISTRY=ghcr.io

# Step 1 - Run Maven Build
FROM ${DOCKER_REGISTRY}/techlib/dspace-dependencies:${DSPACE_VERSION} AS build
#FROM techlib/dspace-dependencies:${DSPACE_VERSION} AS build
ARG TARGET_DIR=dspace-installer
WORKDIR /app
# The dspace-installer directory will be written to /install
RUN mkdir /install \
    && chown -Rv dspace: /install \
    && chown -Rv dspace: /app
USER dspace
# Copy the DSpace source code (from local machine) into the workdir (excluding .dockerignore contents)
ADD --chown=dspace . /app/
# Build DSpace
# Copy the dspace-installer directory to /install.  Clean up the build to keep the docker image small
# Maven flags here ensure that we skip building test environment and skip all code verification checks.
# These flags speed up this compilation as much as reasonably possible.
ENV MAVEN_FLAGS="-P-test-environment -Denforcer.skip=true -Dcheckstyle.skip=true -Dlicense.skip=true -Dxml.skip=true"
RUN mvn --no-transfer-progress package ${MAVEN_FLAGS} && \
  mv /app/dspace/target/${TARGET_DIR}/* /install && \
  mvn clean
# Remove the server webapp to keep image small.
RUN rm -rf /install/webapps/server/

# Step 2 - Run Ant Deploy
FROM docker.io/eclipse-temurin:${JDK_VERSION} AS ant_build
#FROM openjdk:${JDK_VERSION}-slim as ant_build
ARG TARGET_DIR=dspace-installer
# COPY the /install directory from 'build' container to /dspace-src in this container
COPY --from=build /install /dspace-src
WORKDIR /dspace-src
# Create the initial install deployment using ANT
ENV ANT_VERSION=1.10.13
ENV ANT_HOME=/tmp/ant-$ANT_VERSION
ENV PATH=$ANT_HOME/bin:$PATH
# Download and install 'ant'
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl tar \
    && apt-get purge -y --auto-remove \
    && rm -rf /var/lib/apt/lists/*
RUN mkdir $ANT_HOME && \
    curl --silent --show-error --location --fail --retry 5 --output /tmp/apache-ant.tar.gz \
      https://archive.apache.org/dist/ant/binaries/apache-ant-${ANT_VERSION}-bin.tar.gz && \
    tar -zx --strip-components=1 -f /tmp/apache-ant.tar.gz -C $ANT_HOME && \
    rm /tmp/apache-ant.tar.gz
# Run necessary 'ant' deploy scripts
RUN ant init_installation update_configs update_code update_webapps

# Step 3 - Start up DSpace via Runnable JAR
FROM docker.io/eclipse-temurin:${JDK_VERSION}
# NOTE: DSPACE_INSTALL must align with the "dspace.dir" default configuration.
ENV DSPACE_INSTALL=/dspace
# Copy the /dspace directory from 'ant_build' container to /dspace in this container
COPY --from=ant_build /dspace $DSPACE_INSTALL
WORKDIR $DSPACE_INSTALL
# Need host command for "[dspace]/bin/make-handle-config"
RUN apt-get update \
    && apt-get install -y --no-install-recommends host \
    && apt-get purge -y --auto-remove \
    && rm -rf /var/lib/apt/lists/*
# Expose Tomcat port (8080) & Handle Server HTTP port (8000)
EXPOSE 8080 8000
# Give java extra memory (2GB)
ENV JAVA_OPTS=-Xmx2000m
# On startup, run DSpace Runnable JAR
ENTRYPOINT ["java", "-jar", "webapps/server-boot.jar", "--dspace.dir=$DSPACE_INSTALL"]
