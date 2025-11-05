package com.Exercises.Ex1WithDiffClasses;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

public class Counter {

	private static final int numThreads = 10;
	private static final int numMessages = 100;

	public static void main(String[] args) {

		final ActorSystem sys = ActorSystem.create("System");
		final ActorRef counter = sys.actorOf(CounterActor.props(), "counter");

        counter.tell(new DecrementMessage(), ActorRef.noSender());
        counter.tell(new DecrementMessage(), ActorRef.noSender());
        counter.tell(new SimpleMessage(), ActorRef.noSender());
        counter.tell(new DecrementMessage(), ActorRef.noSender());
        counter.tell(new SimpleMessage(), ActorRef.noSender());
        counter.tell(new SimpleMessage(), ActorRef.noSender());

	}

}

