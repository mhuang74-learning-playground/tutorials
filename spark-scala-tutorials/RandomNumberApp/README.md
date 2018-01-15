# README.md

### To Run

```
 sbt clean assembly
 spark-submit --master=local[4] --class SimpleRandomNumberApp target/scala-2.11/SimpleRandomNumberApp-assembly-1.0.jar
```
