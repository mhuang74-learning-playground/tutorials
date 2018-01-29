#!/bin/bash
sbt assembly
spark-submit --class SimpleRandomPrimeNumberApp \
    --master yarn \
    --deploy-mode cluster \
    --driver-memory 2g \
    --executor-memory 8g \
    --executor-cores 4 \
    --num-executors 2 \
    --queue root.users.mhuang \
    --conf spark.yarn.archive=hdfs:///user/mhuang/lib/spark-libs-2.2.0.jar \
    target/scala-2.11/RandomPrimeNumberApp-assembly-1.0.jar \
    1000000 \
    nodebug
