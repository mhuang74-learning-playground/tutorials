#!/bin/bash
sbt assembly
hadoop fs -put -p -f target/scala-2.11/SimpleApp-assembly-1.0.jar oozie-spark/simpleapp/
hadoop fs -put -p -f workflow.xml oozie-spark/simpleapp
