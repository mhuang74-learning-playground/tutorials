package akka.tutorial.pi.actors;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import akka.dispatch.OnSuccess;
import scala.concurrent.Future;
import static akka.dispatch.Futures.future;
import scala.concurrent.duration.Duration;
import akka.tutorial.pi.messages.Calculate;
import akka.tutorial.pi.messages.PiApproximation;
import akka.tutorial.pi.messages.Result;
import akka.tutorial.pi.messages.Work;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.routing.RoundRobinPool;

public class Master extends UntypedActor {
    private final int nrOfMessages;
    private final int nrOfElements;

    private double pi;
    private int nrOfResults;
    private final long start = System.currentTimeMillis();
    private int currentReceivedPercentage = 0;
    private int currentSentPercentage = 0;

    private final ActorRef listener;
    private final ActorRef workerRouter;

    public Master(final int nrOfWorkers, int nrOfMessages, int nrOfElements, ActorRef listener) {
        this.nrOfMessages = nrOfMessages;
        this.nrOfElements = nrOfElements;
        this.listener = listener;

        workerRouter = this.getContext().actorOf(new RoundRobinPool(nrOfWorkers).props(Props.create(Worker.class)),
                "workerRouter");

    }

    public void onReceive(Object message) {
        if (message instanceof Calculate) {

            Future<String> f = future(new Callable<String>() {
                public String call() {
                    for (int start = 0; start < nrOfMessages; start++) {
                        workerRouter.tell(new Work(start, nrOfElements), getSelf());

                        int percentageDone = (int)Math.round(100.0 * start / nrOfMessages);
                        if ((percentageDone - currentSentPercentage) > 0 && (percentageDone - currentSentPercentage) % 5 == 0){
                            currentSentPercentage = percentageDone;
                            System.out.println("... (" + percentageDone + "%)" + "\t Work mesgs sent = " + start);

                        }
                    }
                    return "Done sending all " + nrOfMessages + " Work messages!!";
                }
            },  this.getContext().dispatcher());

            f.onSuccess(new PrintResult<String>(),  this.getContext().dispatcher());

        } else if (message instanceof Result) {
            Result result = (Result) message;
            pi += result.getValue();
            nrOfResults += 1;
           if (nrOfResults == nrOfMessages) {
                // Send the result to the listener
                Duration duration = Duration.create(System.currentTimeMillis() - start, TimeUnit.MILLISECONDS);
                listener.tell(new PiApproximation(pi, duration), getSelf());
                // Stops this actor and all its supervised children
                getContext().stop(getSelf());
            } else {
               int percentageDone = (int)Math.round(100.0 * nrOfResults / nrOfMessages);
               if ((percentageDone - currentReceivedPercentage) > 0 && (percentageDone - currentReceivedPercentage) % 5 == 0){
                   currentReceivedPercentage = percentageDone;
                   System.out.println("... (" + percentageDone + "%)" + "\t pi = " + pi);

               }

           }
        } else {
            unhandled(message);
        }
    }

    public final static class PrintResult<T> extends OnSuccess<T> {
        @Override public final void onSuccess(T t) {
            System.out.println(t);
        }
    }
}
