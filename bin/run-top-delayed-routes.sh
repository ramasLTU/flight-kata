#!/bin/bash
set -e
set -x
set -u

spark-submit \
    --class flight.kata.RunTopDelayedLinks \
    --master yarn-cluster \
    --name TopAirports \
    --num-executors 4 \
    --driver-memory 512M  \
    --driver-java-options "-XX:+UseG1GC" \
    --executor-memory 512M \
    --executor-cores 2 \
    --conf 'spark.executor.extraJavaOptions=-XX:+UseG1GC' \
    flight-kata-1.0-SNAPSHOT.jar ${1}

