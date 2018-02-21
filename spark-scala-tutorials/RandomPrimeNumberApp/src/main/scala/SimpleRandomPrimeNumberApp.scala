/* SimpleRandomNumberApp.scala */

import java.io.{PrintStream}

import org.apache.commons.csv.{CSVFormat, CSVPrinter}
import org.apache.commons.lang.exception.ExceptionUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.util.{Success, Failure, Try}

//import org.apache.logging.log4j.{Level, LogManager}
//import org.apache.logging.log4j.io.IoBuilder
import org.apache.spark.{SparkConf, SparkContext}

object MyFunctions {
  def isPrime(n: Int): Try[Boolean] = {
    val ret = Try {

      if (n == 0)
        throw new RuntimeException("Zeros not allowed!")
      else if (n <= 1)
        false
      else if (n == 2)
        true
      else
        !((2 until n - 1) exists (n % _ == 0))

    }
    ret
  }
}

object LogHolder extends Serializable {
  @transient lazy val log = Logger.getLogger("SimpleRandomPrimeNumberApp")
  @transient lazy val log_csv = Logger.getLogger("SimpleRandomPrimeNumberApp.csv")
}

object SimpleRandomPrimeNumberApp {

  var n = 1000000
  var partitions = 12
  var debug = false

  def main(args: Array[String]) {
    val timeAppStart : Long = System.currentTimeMillis()

    if (args.length == 0) {
      LogHolder.log.warn("Need at least 3 parameter! Using default: n = " + n + ", partitions = " + partitions)
    } else {
      this.n = java.lang.Integer.valueOf(args(0))
      this.partitions = java.lang.Integer.valueOf(args(1))
      this.debug = args(2)!=null && "debug".equals(java.lang.String.valueOf(args(2)))
    }

    val conf = new SparkConf().setAppName("Simple Random Prime Number Application")
    val sc = new SparkContext(conf)

    val accumulable = sc.accumulableCollection(mutable.HashSet[(Any, Throwable)]())

    LogHolder.log.info("**** Calculating Random Prime Number for n = " + this.n + " with partitions = " + this.partitions + ", debug = " + this.debug + " ****")

    // Create a simple RDD containing lots of numbers.
    val r = new scala.util.Random
    //    val randomNumbersRdd = sc.parallelize(1 to n, partitions)
    val randomNumbersRdd = sc.parallelize(for (i <- 1 to n) yield r.nextInt(n), partitions)
    dumpPartitions("randomNumbersRdd", randomNumbersRdd)
    randomNumbersRdd.setName("randomNumbers").cache()

    // NOTE: adding sortBy and distinct takes 3X time due to memory spills..
    // .sortBy(x => x).distinct()

    //// METHOD 1, using Sieve of Eratosthenes

    val timeMethod1Start : Long = System.currentTimeMillis()

    // generate composite numbers up to N using Sieve of Eratosthenes
    // https://en.wikipedia.org/wiki/Sieve_of_Eratosthenes
    val compositeNumbersRdd = sc.parallelize(2 to n, partitions).map(x => (x, (2 to (n / x)))).flatMap(kv => kv._2.map(_ * kv._1))
    dumpPartitions("compositeNumbersRdd", compositeNumbersRdd)

      // NOTE: adding sortBy and distinct takes 3X time due to memory spills..
      // .sortBy(x => x).distinct()


    // get prime numbers by removing all Composite Numbers from candidate pool
    val primeNumbersRdd = randomNumbersRdd.subtract(compositeNumbersRdd)
    dumpPartitions("primeNumbersRdd", primeNumbersRdd)

    // keep only unique primes
    val uniquePrimeNumbersRdd = primeNumbersRdd.distinct()
    dumpPartitions("uniquePrimeNumbersRdd", uniquePrimeNumbersRdd)

    // sorting need to come AFTER distinct() !
    val sortedUniquePrimeNumbersRdd = uniquePrimeNumbersRdd.sortBy(x => x, true)
    dumpPartitions("sortedUniquePrimeNumbersRdd", sortedUniquePrimeNumbersRdd)


    // collect results into Array

    val primeNumberIntArray: Array[java.lang.Integer] = sortedUniquePrimeNumbersRdd.collect() map java.lang.Integer.valueOf

    val timeMethod1End : Long = System.currentTimeMillis()
    LogHolder.log.info("Using Sieve of Eratosthenes took " + (timeMethod1End-timeMethod1Start)/1000 + " secs")

    //// METHOD 2, brute force

    val timeMethod2Start : Long = System.currentTimeMillis()

    val primeNumbers2Rdd = randomNumbersRdd.flatMap(n => {
      val isAPrime = MyFunctions.isPrime(n)
      val trial = isAPrime match {
        case Failure(t) =>
          // push exception to accumulable
          // both data and throwable saved
          accumulable += (n, t)
          None
        case Success(true) => Some(n)
        case _ => None
      }
      trial
    })

    dumpPartitions("(brute force) primeNumbers2Rdd", primeNumbers2Rdd)


    // keep only unique primes
    val uniquePrimeNumbers2Rdd = primeNumbers2Rdd.distinct()
    dumpPartitions("(brute force) uniquePrimeNumbers2Rdd", uniquePrimeNumbers2Rdd)

    // sorting need to come AFTER distinct() !
    val sortedUniquePrimeNumbers2Rdd = uniquePrimeNumbers2Rdd.sortBy(x => x, true)
    dumpPartitions("(brute force) sortedUniquePrimeNumbers2Rdd", sortedUniquePrimeNumbers2Rdd)

    val primeNumber2IntArray: Array[java.lang.Integer] = sortedUniquePrimeNumbers2Rdd.collect() map java.lang.Integer.valueOf

    accumulable.value.foreach{case (i, e) => {
        LogHolder.log.warn(" Bad number: " + i)
        LogHolder.log.warn(ExceptionUtils.getStackTrace(e))
      }
    }

    val timeMethod2End : Long = System.currentTimeMillis()
    LogHolder.log.info("Using Brute Force took " + (timeMethod2End-timeMethod2Start)/1000 + " secs")

    //// OUTPUT

    // Export to CSV (using Java CSV Commons library).

    val stream = new PrintStream(new LoggingOutputStream(LogHolder.log_csv, Level.INFO))
    val printer = new CSVPrinter(stream, CSVFormat.DEFAULT)

    printer.printRecord(" ==== Random Prime Numbers from 1 to ==== " + n)

    printer.printRecord(" == Sieve of Eratosthenes == ")
    val csv_rows = primeNumberIntArray.grouped(40)
    csv_rows.foreach(row => printer.printRecords(row))

    printer.printRecord(" == Brute Force == ")
    val csv_rows2 = primeNumber2IntArray.grouped(40)
    csv_rows2.foreach(row => printer.printRecords(row))

    printer.flush()
    printer.close()



    val timeAppEnd : Long = System.currentTimeMillis()

    LogHolder.log.info("****  Finished Random Prime Number for n = " + n + ". Took " + (timeAppEnd-timeAppStart)/1000 + " secs ****")

  }

  def dumpPartitions(rddName: String, rdd: RDD[Int]) = {

    if (this.debug) {
      LogHolder.log.debug(rddName + " - size:" + rdd.count() + ", partitions:" + rdd.partitions.size)

      // make sure rdd is cached after count() action!
      rdd.cache()

      rdd.mapPartitionsWithIndex {
        (index, iterator) => {
          val iteratorList = iterator.toList
          LogHolder.log.debug(rddName + "[part-" + index + "] - size:" + iteratorList.size + ", samples:" + iteratorList.take(5))
          iterator
        }
      }.take(1)

    } else {
      LogHolder.log.info(rddName + " - partitions:" + rdd.partitions.size)
    }

  }

}




