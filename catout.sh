#!/usr/bin/env bash
#
echo "ID,X,Y,CLUSTER" > ~/Share/moon.txt
cat /tmp/output/part-* >> ~/Share/moon.txt
rm -rf /tmp/output
