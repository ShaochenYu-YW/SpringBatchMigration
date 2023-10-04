#!/bin/bash

if [[ -f "config/service.env" ]]
then
  source config/service.env
fi

if [[ -z "${JAVA_OPTS}" ]]
then
  export JAVA_OPTS="-XX:MaxRAMPercentage=50"
fi

echo $JAVA_OPTS
exec java ${JAVA_OPTS} -jar advretrofill.jar "$@"
