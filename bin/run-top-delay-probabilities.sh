#!/bin/bash
set -e
set -x
set -u

spark-submit \
    --class flight.kata.RunTopDelayProbabilities \
    --master yarn-cluster \
    --name TopDelayProbabilities \
    --num-executors 4 \
    --driver-memory 1G  \
    --driver-java-options "-XX:+UseG1GC" \
    --executor-memory 2G \
    --executor-cores 4 \
    --conf 'spark.executor.extraJavaOptions=-XX:+UseG1GC' \
    flight-kata-1.0-SNAPSHOT.jar ${1}

