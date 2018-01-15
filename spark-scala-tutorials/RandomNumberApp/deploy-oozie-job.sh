#!/bin/bash
sbt assembly
hadoop fs -put -p -f target/scala-2.11/SimpleRandomNumberApp-assembly-1.0.jar /user/mhuang/oozie-spark/simple-randomnumber-app/
hadoop fs -put -p -f workflows/workflow.xml /user/mhuang/oozie-spark/simple-randomnumber-app
