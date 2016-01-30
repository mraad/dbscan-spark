#!/usr/bin/env bash

export SPARK_LOCAL_IP=192.168.1.5

rm -rf /tmp/output

spark-submit\
 --master "local[*]"\
 --driver-java-options="-server -Xms1g -Xmx16g"\
 target/dbscan-spark-0.1.jar\
 ss-local.properties

# --driver-memory 16G\
# --executor-memory 16G\
