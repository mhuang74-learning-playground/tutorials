package akka.tutorial.pi;

import akka.tutorial.pi.actors.Listener;
import akka.tutorial.pi.actors.Master;
import akka.tutorial.pi.messages.Calculate;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.japi.Creator;
import kamon.Kamon;

public class Pi {
    public static void main(String[] args) {
        Kamon.start();

        try {
            Thread.sleep(3000);
        } catch(Exception e) { }

        Pi pi = new Pi();
        pi.calculate(10, 1000000, 1000000);

        // This application wont terminate unless you shutdown Kamon.
        Kamon.shutdown();
    }

    // actors and messages ...

    public void calculate(final int nrOfWorkers, final int nrOfMessages, final int nrOfElements) {
        // Create an Akka system
        ActorSystem system = ActorSystem.create("PiSystem");

        // create the result listener, which will print the result and shutdown the system
        final ActorRef listener = system.actorOf(Props.create(Listener.class), "listener");

        // create the master
        ActorRef master = system.actorOf(Props.create(new MasterActorCreator(nrOfWorkers, nrOfMessages, nrOfElements, listener)), "master");

        // start the calculation
        master.tell(new Calculate(), null);

    }
    
    
    static class MasterActorCreator implements Creator<Master> {
        private final int nrOfWorkers;
        private final int nrOfMessages;
        private final int nrOfElements;

        private final ActorRef listener;
        public MasterActorCreator(final int nrOfWorkers, int nrOfMessages, int nrOfElements, ActorRef listener) {
            this.nrOfWorkers = nrOfWorkers;
            this.nrOfMessages = nrOfMessages;
            this.nrOfElements = nrOfElements;
            this.listener = listener;
        }
        
        
        @Override public Master create() {
          return new Master(nrOfWorkers, nrOfMessages, nrOfElements, listener);
        }
      }
}
    
   
