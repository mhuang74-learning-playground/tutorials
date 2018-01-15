#!/bin/bash
sbt assembly
spark-submit --class SimpleRandomPrimeNumberApp \
    --master yarn \
    --deploy-mode cluster \
    --driver-memory 4g \
    --executor-memory 2g \
    --executor-cores 1 \
    --queue root.prioritySpark \
    --conf spark.yarn.archive=hdfs:///user/mhuang/lib/spark-libs-2.2.0.jar \
    target/scala-2.11/RandomPrimeNumberApp-assembly-1.0.jar \
    100000 \
    nodebug
