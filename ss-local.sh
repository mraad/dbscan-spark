#!/usr/bin/env bash

export SPARK_LOCAL_IP=localhost

rm -rf /tmp/dbscan

cat << EOF > /tmp/dbscan.properties
spark.ui.enabled=false
# spark.ui.showConsoleProgress=true
# spark.shuffle.compress=false
# spark.shuffle.spill.compress=false
# spark.broadcast.compress=false

input.path=src/test/resources/smiley2.txt
output.path=/tmp/dbscan

dbscan.eps=250000
dbscan.min.points=5
#
# Cell size by default is 10 * eps
#
# dbscan.cell.size=50
EOF

spark-submit\
 --master "local[*]"\
 --driver-memory 16G\
 --executor-memory 16G\
 target/dbscan-spark-0.6.jar\
 /tmp/dbscan.properties

echo "ID,X,Y,DBSCAN" > /tmp/dbscan.csv
cat /tmp/dbscan/part-* >> /tmp/dbscan.csv
rm -rf /tmp/dbscan
