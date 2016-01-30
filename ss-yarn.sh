#!/usr/bin/env bash

hadoop fs -rm -r -skipTrash /tmp/output

spark-submit\
 --master yarn-client\
 --num-executors 1\
 --executor-memory 4g\
 --executor-cores 4\
 target/dbscan-spark-0.1.jar\
 ss-yarn.properties
