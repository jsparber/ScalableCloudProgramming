#!/bin/bash

set -e
set -x

${SPARK_HOME}/bin/spark-submit \
    --class "${APP_CLASS}" \
    --master spark://master:7077  \
    --properties-file ${APP_BASE}/twitter.properties  \
    ${APP_BASE}/spark-app.jar

