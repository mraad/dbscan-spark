#!/usr/bin/env bash

hdfs dfs -rm -r -skipTrash /tmp/input
hdfs dfs -rm -r -skipTrash /tmp/output

hdfs dfs -mkdir /tmp/input
hdfs dfs -put src/test/resources/smiley2.txt /tmp/input

spark-submit\
 --master yarn\
 --num-executors 1\
 --executor-memory 2g\
 target/dbscan-spark-0.6.jar\
 ss-yarn.properties

hdfs dfs -cat /tmp/output/part-* > target/smiley2.csv
