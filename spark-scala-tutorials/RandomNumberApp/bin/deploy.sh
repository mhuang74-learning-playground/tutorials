#!/bin/bash
sbt assembly
sudo HADOOP_USER_NAME=dev hadoop fs -put -p -f target/scala-2.11/SimpleRandomNumberApp-assembly-1.0.jar /user/dev/oozie-spark/simple-randomnumber-app/
sudo HADOOP_USER_NAME=dev hadoop fs -put -p -f workflow.xml /user/dev/oozie-spark/simple-randomnumber-app
