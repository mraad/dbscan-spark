#!/usr/bin/env bash

export SPARK_LOCAL_IP=localhost

rm -rf /tmp/output

spark-submit\
 --master "local[*]"\
 --driver-java-options="-server -Xms1g -Xmx16g"\
 target/dbscan-spark-0.3.jar\
 ss-local.properties

# --driver-memory 16G\
# --executor-memory 16G\
