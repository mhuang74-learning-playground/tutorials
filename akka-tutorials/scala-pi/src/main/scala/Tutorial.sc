import akka.actor.{Props, Actor}
import com.sun.tools.javac.comp.Annotate.Worker
sealed trait PiMessage
case object Calculate extends PiMessage
case class Work(start: Int, nbOfElements: Int) extends PiMessage
case class Result(value: Double) extends PiMessage
class Worker extends Actor {
  def receive = {
    case Work(start, nbOfElements) =>
      sender ! Result(calculatePiFor(start, start + nbOfElements))
  }

  def calculatePiFor(start: Int, stop: Int): Double = {
    (start until stop) map { i =>
      4.0 * (1 - (i % 2) * 2) / (2 * i + 1)
    } sum
  }
}
val foo = context.actorOf(Worker)