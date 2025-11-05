package com.Exercises.Ex1MessageSelector;

import akka.actor.AbstractActorWithStash;
import akka.actor.Props;

public class CounterActor extends AbstractActorWithStash {

	private int counter;

	public CounterActor() {
		this.counter = 0;
	}

	@Override
	public Receive createReceive() {
        System.out.println("Entro in createReceive");
		return receiveBuilder()
                .match(Message.class, this::onMessage)
                .build();
	}

	void onMessage(Message msg) {
        if(msg.isValue()) {
            counter++;
            System.out.println("Counter received: " + counter);
        }
        else {
            counter--;
            System.out.println("Counter received: " + counter);
        }
	}

	static Props props() {
		return Props.create(CounterActor.class);
	}

}
