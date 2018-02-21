#!/bin/bash
sbt clean assembly
spark-submit --verbose --class SimpleRandomPrimeNumberApp \
    --master=local[8] \
    --deploy-mode client \
    --driver-memory 2g \
    --executor-memory 2g \
    --executor-cores 3 \
    --num-executors 5 \
    --driver-class-path target/scala-2.11/RandomPrimeNumberApp-assembly-1.0.jar \
    --driver-java-options "-Dlog4j.configuration=log4j.properties" \
    --conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=log4j.properties" \
    --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
    --conf "spark.io.compression.codec=snappy" \
    target/scala-2.11/RandomPrimeNumberApp-assembly-1.0.jar \
    100000 \
    8 \
    debug
