package com.Exercises.Ex1WithDiffClasses;

import akka.actor.AbstractActorWithStash;
import akka.actor.Props;

public class CounterActor extends AbstractActorWithStash {

	private int counter;

	public CounterActor() {
		this.counter = 0;
	}

	@Override
	public Receive createReceive() {
		return receiveBuilder()
                .match(SimpleMessage.class, this::onMessage)
                .match(DecrementMessage.class, this::onMessageDecrement)
                .build();
	}

	void onMessage(SimpleMessage msg) {
        counter++;
        System.out.println("Counter increased to " + counter);
	}

    void onMessageDecrement(DecrementMessage msg) {
            counter--;
            System.out.println("Counter increased to " + counter);
    }

	public static Props props() {
		return Props.create(CounterActor.class);
	}

}

