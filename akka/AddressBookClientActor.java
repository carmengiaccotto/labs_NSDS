package com.Exercises.Lab25;
import static akka.pattern.Patterns.ask;
import static java.util.concurrent.TimeUnit.SECONDS;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.pattern.Patterns;

import java.time.Duration;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeoutException;

public class AddressBookClientActor extends AbstractActor {

	private scala.concurrent.duration.Duration timeout = scala.concurrent.duration.Duration.create(3, SECONDS);

    private ActorRef bookBalancer;

	@Override
	public Receive createReceive() {
        return receiveBuilder()
                .match(ConfigMsg.class, this::onConfig)
                .match(PutMsg.class, this::putEntry)
                .match(GetMsg.class, this::query)
                .build();
	}

    private void onConfig(ConfigMsg cfg) {
        this.bookBalancer = cfg.getBalancer();
    }

	void putEntry(PutMsg msg) {
        System.out.println("CLIENT: Sending new entry " + msg.getName() + " - " + msg.getEmail());
        bookBalancer.tell(msg, getSelf());
	}

	void query(GetMsg msg) {
        System.out.println("CLIENT: Issuing query for " + msg.getName());

        try {
            scala.concurrent.Future<Object> waitingForGetMsg = ask(bookBalancer, msg, 5000);
            Object result = waitingForGetMsg.result(timeout, null);

            if (result instanceof ReplyMsg) {
                ReplyMsg email = (ReplyMsg) result;
                if (email.getEmail() == null) {
                    System.out.println("CLIENT: Received reply, no email found!");
                } else {
                    System.out.println("CLIENT: Received reply " + email.getEmail());
                }
            } else if (result instanceof TimeoutMsg) {
                System.out.println("CLIENT: Received timeout, both copies are resting!");
            }

        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (TimeoutException e) {
            System.out.println("CLIENT: No reply from the balancer!");
        }
		
	}

	static Props props() {
		return Props.create(AddressBookClientActor.class);
	}

}
