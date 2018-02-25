#!/bin/bash
sbt clean assembly
spark-submit --verbose --class SimpleRandomPrimeNumberApp \
    --master yarn \
    --deploy-mode cluster \
    --driver-memory 2g \
    --executor-memory 2g \
    --executor-cores 3 \
    --num-executors 5 \
    --queue root.users.mhuang \
    --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
    --conf "spark.io.compression.codec=snappy" \
    --conf spark.yarn.archive=hdfs:///user/spark/share/lib/spark-libs-2.2.0.jar \
    --conf spark.eventLog.enabled=true \
    --conf spark.eventLog.dir=hdfs:///user/spark/spark2ApplicationHistory \
    --conf spark.eventLog.compress=true \
    --driver-class-path RandomPrimeNumberApp-assembly-1.0.jar \
    --files target/scala-2.11/classes/log4j.properties \
    --driver-java-options "-Dlog4j.configuration=log4j.properties" \
    --conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=log4j.properties" \
    target/scala-2.11/RandomPrimeNumberApp-assembly-1.0.jar \
    1000000 \
    45 \
    debug

