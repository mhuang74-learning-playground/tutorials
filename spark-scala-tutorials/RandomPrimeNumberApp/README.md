# README.md

### To Run

```
 sbt clean assembly
 spark-submit --master=local[4] --class SimpleRandomPrimeNumberApp target/scala-2.11/RandomPrimeNumberApp-assembly-1.0.jar 100000 nodebug
```
