/* SimpleRandomNumberApp.scala */

import java.io.{PrintStream}

import org.apache.commons.csv.{CSVFormat, CSVPrinter}
import org.apache.log4j.{Level, Logger}

//import org.apache.logging.log4j.{Level, LogManager}
//import org.apache.logging.log4j.io.IoBuilder
import org.apache.spark.{SparkConf, SparkContext}

object SimpleRandomPrimeNumberApp {
  def main(args: Array[String]) {
    val time1 : Long = System.currentTimeMillis()

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

    log.info("**** Calculating Random Prime Number for n = " + n + " ****")

    // generate composite numbers up to N using Sieve of Eratosthenes
    // https://en.wikipedia.org/wiki/Sieve_of_Eratosthenes
    val compositeNumbersRdd = sc.parallelize(2 to n, 8).map(x => (x, (2 to (n / x)))).repartition(8).flatMap(kv => kv._2.map(_ * kv._1))
      // NOTE: adding sortBy and distinct takes 3X time due to memory spills..
      // .sortBy(x => x).distinct()

    if (debug) {
      val compositeNumbersRddList = compositeNumbersRdd.take(10).toList
      log.debug("Some composite numbers: " + compositeNumbersRdd.take(10).toList)
    }

    // Create a simple RDD containing lots of numbers.
    val r = new scala.util.Random
    //    val randomNumbersRdd = sc.parallelize(1 to n, 8)
    val randomNumbersRdd = sc.parallelize(for (i <- 1 to n) yield r.nextInt(n), 8)
      // NOTE: adding sortBy and distinct takes 3X time due to memory spills..
      // .sortBy(x => x).distinct()

    if (debug) {
      val randomNumbersRddList = randomNumbersRdd.take(10).toList
      log.debug("Some random numbers: " + randomNumbersRdd.take(10).toList)
    }

    // find prime numbers
    val primeNumbersRdd = randomNumbersRdd.subtract(compositeNumbersRdd)
    if (debug) {
      val primeNumbersRddList = primeNumbersRdd.take(10).toList
    }

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

    // Convert this RDD into CSV (using Java CSV Commons library).

    log.debug("Before RDD collect")
    val primeNumberIntArray: Array[java.lang.Integer] = sortedUniquePrimeNumbersRdd.collect() map java.lang.Integer.valueOf
    log.debug("After RDD collect")

    val stream = new PrintStream(new LoggingOutputStream(log_csv, Level.INFO))
    val printer = new CSVPrinter(stream, CSVFormat.DEFAULT)

    val csv_rows = primeNumberIntArray.grouped(40)
    printer.printRecord("Random Prime Numbers from 1 to " + n)
    csv_rows.foreach(row => printer.printRecords(row))

    printer.flush()
    printer.close()

    val time2 : Long = System.currentTimeMillis()

    log.info("****  Finished Random Prime Number for n = " + n + ". Took " + (time2-time1)/1000 + " secs ****")

  }

  def isPrime(n: Int) : Boolean = {
    if (n <= 1)
      false
    else if (n == 2)
      true
    else
      !((2 until n-1) exists (n % _ == 0))
  }
}


