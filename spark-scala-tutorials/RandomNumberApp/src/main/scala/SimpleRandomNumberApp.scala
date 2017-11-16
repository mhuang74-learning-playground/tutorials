/* SimpleApp.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import org.apache.commons.csv._
import org.apache.logging.log4j._

object SimpleRandomNumberApp {
  def main(args: Array[String]) {

    val log = LogManager.getRootLogger

    val conf = new SparkConf().setAppName("Simple Random Number Application")
    val sc = new SparkContext(conf)

    log.info("Simple Random Number Application started...")

    // Create a simple RDD containing lots of numbers.
    val r = new scala.util.Random
    val numbers = 1 to r.nextInt(1000000) by 7 toArray
    val numbersListRdd = sc.parallelize(numbers)
 
    // Convert this RDD into CSV (using Java CSV Commons library).
    val printer = new CSVPrinter(Console.out, CSVFormat.DEFAULT)
    val javaArray: Array[java.lang.Integer] = numbersListRdd.collect() map java.lang.Integer.valueOf
    printer.printRecords(javaArray)
    printer.flush()
    printer.close()


    log.info("Simple Random Number Application ending...")

  }
}


