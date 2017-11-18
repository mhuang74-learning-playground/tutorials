/* SimpleRandomNumberApp.scala */

import java.io.{PrintStream}

import org.apache.commons.csv.{CSVFormat, CSVPrinter}
import org.apache.commons.lang.exception.ExceptionUtils
import org.apache.log4j.{Level, Logger}

import scala.collection.mutable
import scala.util.{Success, Failure, Try}

//import org.apache.logging.log4j.{Level, LogManager}
//import org.apache.logging.log4j.io.IoBuilder
import org.apache.spark.{SparkConf, SparkContext}

object SimpleRandomPrimeNumberApp {
  def main(args: Array[String]) {
    val timeAppStart : Long = System.currentTimeMillis()

    val log = Logger.getLogger("SimpleRandomPrimeNumberApp")
    val log_csv = Logger.getLogger("SimpleRandomPrimeNumberApp.csv")

    var n = 1000000
    var debug = false

    if (args.length == 0) {
      log.warn("Need at least one parameter! Using default: n = " + n)
    } else {
      n = java.lang.Integer.valueOf(args(0))
      debug = "debug".equals(java.lang.String.valueOf(args(1)))
    }

    val conf = new SparkConf().setAppName("Simple Random Prime Number Application")
    val sc = new SparkContext(conf)

    val accumulable = sc.accumulableCollection(mutable.HashSet[(Any, Throwable)]())

    log.info("**** Calculating Random Prime Number for n = " + n + " ****")

    // Create a simple RDD containing lots of numbers.
    val r = new scala.util.Random
    //    val randomNumbersRdd = sc.parallelize(1 to n, 8)
    val randomNumbersRdd = sc.parallelize(for (i <- 1 to n) yield r.nextInt(n), 8)
    randomNumbersRdd.cache()
    // NOTE: adding sortBy and distinct takes 3X time due to memory spills..
    // .sortBy(x => x).distinct()

    if (debug) {
      val randomNumbersRddList = randomNumbersRdd.take(10).toList
      log.debug("Some random numbers: " + randomNumbersRdd.take(10).toList)
    }

    //// METHOD 1, using Sieve of Eratosthenes

    val timeMethod1Start : Long = System.currentTimeMillis()

    // generate composite numbers up to N using Sieve of Eratosthenes
    // https://en.wikipedia.org/wiki/Sieve_of_Eratosthenes
    val compositeNumbersRdd = sc.parallelize(2 to n, 8).map(x => (x, (2 to (n / x)))).repartition(8).flatMap(kv => kv._2.map(_ * kv._1))
      // NOTE: adding sortBy and distinct takes 3X time due to memory spills..
      // .sortBy(x => x).distinct()

    if (debug) {
      val compositeNumbersRddList = compositeNumbersRdd.take(10).toList
      log.debug("Some composite numbers: " + compositeNumbersRdd.take(10).toList)
    }

    // get prime numbers by removing all Composite Numbers from candidate pool
    val primeNumbersRdd = randomNumbersRdd.subtract(compositeNumbersRdd)

    if (debug) {
      val primeNumbersRddList = primeNumbersRdd.take(10).toList
    }

    // keep only unique primes
    val uniquePrimeNumbersRdd = primeNumbersRdd/*.repartition(4)*/.distinct()

    if (debug) {
      val uniquePrimeNumbersRddList = uniquePrimeNumbersRdd.take(10).toList
    }

    // sorting need to come AFTER distinct() !
    val sortedUniquePrimeNumbersRdd = uniquePrimeNumbersRdd/*.repartition(4)*/.sortBy(x => x, true, 8)

    if (debug) {
      val sortedUniquePrimeNumbersRddList = sortedUniquePrimeNumbersRdd.take(10).toList
      log.debug("Sample of Prime Numbers found: " + sortedUniquePrimeNumbersRdd.take(10).toList)
    }

    // collect results into Array

    val primeNumberIntArray: Array[java.lang.Integer] = sortedUniquePrimeNumbersRdd.collect() map java.lang.Integer.valueOf

    val timeMethod1End : Long = System.currentTimeMillis()
    log.info("Using Sieve of Eratosthenes took " + (timeMethod1End-timeMethod1Start)/1000 + " secs")

    //// METHOD 2, brute force

    val timeMethod2Start : Long = System.currentTimeMillis()

//    val primeNumbers2Rdd = randomNumbersRdd.filter(isPrime(_).toOption == Success(true))
//    val primeNumbers2Rdd = randomNumbersRdd.filter(n => Try(isPrime(n)) == Success(true))
    val primeNumbers2Rdd = randomNumbersRdd.flatMap(n => {
      val isAPrime = isPrime(n)
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

    if (debug) {
      val primeNumbers2RddList = primeNumbers2Rdd.take(10).toList
      log.debug("Sample of Prime Numbers found via brute force: " + primeNumbers2Rdd.take(10).toList)
    }

    // keep only unique primes
    val uniquePrimeNumbers2Rdd = primeNumbers2Rdd/*.repartition(4)*/.distinct()

    if (debug) {
      val uniquePrimeNumbers2RddList = uniquePrimeNumbers2Rdd.take(10).toList
    }

    // sorting need to come AFTER distinct() !
    val sortedUniquePrimeNumbers2Rdd = uniquePrimeNumbers2Rdd/*.repartition(4)*/.sortBy(x => x, true, 8)

    if (debug) {
      val sortedUniquePrimeNumbers2RddList = sortedUniquePrimeNumbers2Rdd.take(10).toList
      log.debug("Sample of Prime Numbers found: " + sortedUniquePrimeNumbers2Rdd.take(10).toList)
    }

    val primeNumber2IntArray: Array[java.lang.Integer] = sortedUniquePrimeNumbers2Rdd.collect() map java.lang.Integer.valueOf

    accumulable.value.foreach{case (i, e) => {
        log.warn(" Bad number: " + i)
        log.warn(ExceptionUtils.getStackTrace(e))
      }
    }

    val timeMethod2End : Long = System.currentTimeMillis()
    log.info("Using Brute Force took " + (timeMethod2End-timeMethod2Start)/1000 + " secs")

    //// OUTPUT

    // Export to CSV (using Java CSV Commons library).

    val stream = new PrintStream(new LoggingOutputStream(log_csv, Level.INFO))
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

    log.info("****  Finished Random Prime Number for n = " + n + ". Took " + (timeAppEnd-timeAppStart)/1000 + " secs ****")

  }

  def isPrime(n: Int) : Try[Boolean] = {
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


