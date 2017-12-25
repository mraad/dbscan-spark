#!/usr/bin/env bash

export SPARK_LOCAL_IP=localhost

rm -rf /tmp/output

spark-submit\
 --master local[*]\
 --num-executors 1\
 --driver-memory 16G\
 --executor-memory 16G\
 target/dbscan-spark-0.4.jar\
 moons.properties

if [ -d /tmp/output ] ; then
    cat /tmp/output/part-* > /tmp/parts.csv
fi
