FROM gradle:7-jdk17 AS build

RUN mkdir /build
WORKDIR /build

COPY build.gradle.kts settings.gradle.kts /build/
COPY src /build/src

RUN gradle build --no-daemon -x test

FROM openjdk:21-jdk-slim

RUN \
  apt-get update -y && \
  apt-get upgrade --no-install-recommends -y && \
  apt-get install --no-install-recommends -y \
    bash \
    wget \
    dumb-init \
    python3 \
    python3-pip && \
  wget -O dd-java-agent.jar https://repo1.maven.org/maven2/com/datadoghq/dd-java-agent/0.99.0/dd-java-agent-0.99.0.jar && \
  echo "1abe9584198a2c0f91e22c79281bc15f8f2488aa dd-java-agent.jar" | sha1sum -c && \
  groupadd -r labelbox && \
  useradd -m --no-log-init -r -g labelbox labelbox && \
  # cleanup
  echo "**** cleanup ****" && \
  apt-get autoremove -y && \
  apt-get clean && \
  rm -rf \
    /tmp/* \
    /var/lib/apt/lists/* \
    /var/tmp/*


RUN mkdir /app
COPY --from=build /build/build/libs/advretrofill-0.0.1-SNAPSHOT.jar /app/advretrofill.jar
COPY docker/run_server.sh /app/run_server.sh

USER labelbox
WORKDIR /app
ENTRYPOINT ["/usr/bin/dumb-init", "--", "./run_server.sh"]