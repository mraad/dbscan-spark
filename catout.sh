#!/usr/bin/env bash
#
echo "ID,X,Y,CLUSTER" > ~/Share/output.csv
cat /tmp/output/part-* >> ~/Share/output.csv
rm -rf /tmp/output
