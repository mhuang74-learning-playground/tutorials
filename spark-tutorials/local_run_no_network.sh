#!/bin/bash
HADOOP_CONF_DIR= PYSPARK_PYTHON=/Users/mhuang/anaconda2/bin/python spark-submit --master local[2] --deploy-mode client --executor-memory 512M --name wordcount --conf "spark.app.id=wordcount" --conf "spark.eventLog.enabled=false" nouncount2.py $1 $2
