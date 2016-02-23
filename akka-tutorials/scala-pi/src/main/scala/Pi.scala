import akka.actor.{ActorSystem, ActorRef, Actor, Props}
import akka.routing.{RoundRobinRoutingLogic, ActorRefRoutee, Broadcast, Router}

object Main {

  sealed trait PiMessage

  case class Calculate(listener: ActorRef) extends PiMessage

  case class Work(start: Int, nbOfElements: Int) extends PiMessage

  case class Result(value: Double) extends PiMessage



  class Worker extends Actor {
    def receive = {
      case Work(start, nbOfElements) =>

        val res = calculatePiFor(start, start + nbOfElements)

        // println("Received Work: sender=" + sender() + ", start=" + start + ", count=" + nbOfElements + ", res=" + res)

        sender() ! Result(res)
    }

    def calculatePiFor(start: Int, stop: Int): Double = {
      (start until stop) map { i =>
        4.0 * (1 - (i % 2) * 2) / (2 * i + 1)
      } sum
    }
  }

  class Master(nbOfWorkers: Int, nbOfMessages: Int, nbOfElements: Int) extends Actor {

    var pi: Double = 0.0
    var nbOfResults: Int = 0
    var start: Long = 0


    var router = {
      val routees = Vector.fill(nbOfWorkers) {
        val r = context.actorOf(Props[Worker])
        context watch r
        ActorRefRoutee(r)
      }
      Router(RoundRobinRoutingLogic(), routees)
    }


    def receive = {

       case Calculate(listener) => {

        println("Received Calculate! listener=" + listener)

        for (i <- 0 until nbOfMessages) {
          router.route(Work(i * nbOfElements, nbOfElements), listener)
        }
       }

      case Result(value) => {

        // println("Received Result = " + value)

        pi += value
        nbOfResults += 1

        if(nbOfResults == nbOfMessages) {
          println("All " + nbOfMessages + " results received. Terminating...")
          context.system.terminate()
        }
      }
    }

    override def preStart(): Unit = {
      start = System.currentTimeMillis()
      println("Master started at: " + start)
    }

    override def postStop(): Unit = {
      println(
        "\n\tPi estimate: \t\t%s\n\tCalculation time: \t%s millis"
        .format(pi, System.currentTimeMillis() - start)
      )
    }
  }

  def calculate(nbOfWorkers: Int, nbOfElements: Int, nbOfMessages: Int): Unit = {
    val system = ActorSystem("PiCalculator")

    val master = system.actorOf(Props(
      new Master(nbOfWorkers, nbOfMessages, nbOfElements)
    ))

    master ! Calculate(master)
  }

  def main(args: Array[String]): Unit = {
    calculate(nbOfWorkers = 4, nbOfElements = 1000, nbOfMessages = 100)
  }


}