import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by mhuang on 11/16/17.
  */
object WordCountApp {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Word Count")
    val sc = new SparkContext(conf)

    val lines = sc.parallelize(
      Seq(
        "Google Docs went down for a little over an hour today for what Google says was a “significant subset of users.” For a product with a user base that reaches into the hundreds of millions at a minimum, that’s certain to mean a huge number of people who experienced a disruption.",
        "Oddly, the outage was limited only to Google Docs — other portions of Drive and G Suite were still working for everyone. And for the people who were still able to access Docs, there didn’t seem to be any problems at all.",
        "Those in the unfortunate affected group were essentially locked out of their documents from shortly before 4PM ET to shortly after 5PM ET. Google’s services remain up nearly all of the time, so even the slightest outage is a surprise. It also occurred in the middle of the US work and school day, making the downtime a bit more disruptive as it left people without the ability to access documents they’d been writing and editing.",
        "Google first acknowledged the issue at 3:48PM ET, writing that it was “aware of a problem” and that left some people “unable to access Google Docs.” An hour later, it said the problem had been resolved for “some users.” And by 5:10PM ET, Google said everything was in the clear. “We apologize for the inconvenience and thank you for your patience and continued support,” the company wrote. It didn’t comment on what caused the outage."
      )
    )

    val counts = lines.flatMap(
      line => line.split(" ")
    )
      .map(word => (word, 1))
      .reduceByKey(_+_)

    counts.foreach(println)

  }
}
