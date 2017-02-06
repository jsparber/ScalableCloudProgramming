#!/bin/bash

set -e
set -x

${SPARK_HOME}/bin/spark-submit \
    --class "${APP_CLASS}" \
    --master spark://master:7077  \
    ${APP_BASE}/spark-app.jar

