package com.Exercises.Lab25;
import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;

import java.util.concurrent.TimeoutException;

import static akka.pattern.Patterns.ask;
import static java.util.concurrent.TimeUnit.SECONDS;

public class AddressBookBalancerActor extends AbstractActor {

	ActorRef worker1 = null;
	ActorRef worker0 = null;

	public AddressBookBalancerActor() {
	}

	@Override
	public Receive createReceive() {
		return receiveBuilder()
                .match(ConfigMsg.class, this::onConfigMsg)
                .match(PutMsg.class, this::storeEntry)
                .match(GetMsg.class, this::routeQuery)
                .build();
	}

	int splitByInitial(String s) {
		char firstChar = s.charAt(0);

		char upper = Character.toUpperCase(firstChar);

		if (upper >= 'A' && upper <= 'M') {
			return 0;
		} else {
			return 1;
		}
	}

    private  void onConfigMsg(ConfigMsg msg){
        this.worker0 = msg.getRefWorker0();
        this.worker1 = msg.getRefWorker1();
    }

	void routeQuery(GetMsg msg) throws InterruptedException, TimeoutException {
		
		System.out.println("BALANCER: Received query for name " + msg.getName());
        if (splitByInitial(msg.getName()) == 0){
            try {
                scala.concurrent.Future<Object> waitingForGetMsg = ask(worker0, msg, 1000);
                scala.concurrent.duration.Duration timeout = scala.concurrent.duration.Duration.create(1, SECONDS);

                Object res = waitingForGetMsg.result(timeout, null);
                ReplyMsg replyMsg = (ReplyMsg) res;
                getSender().tell(replyMsg, getSelf());

            } catch (TimeoutException teWorker0) {
                System.out.println("BALANCER: Primary copy query for name " + msg.getName() + " is resting!");

                try {
                    scala.concurrent.Future<Object> waitingForGetMsg1 = ask(worker1, msg, 1000);
                    scala.concurrent.duration.Duration timeout = scala.concurrent.duration.Duration.create(1, SECONDS);
                    Object res2 = waitingForGetMsg1.result(timeout, null);
                    ReplyMsg replyMsg2 = (ReplyMsg) res2;
                    getSender().tell(replyMsg2, getSelf());

                } catch (TimeoutException teWorker1) {
                    System.out.println("BALANCER: Both copies are resting for name " + msg.getName() + "!");
                    getSender().tell(new TimeoutMsg(), getSelf());
                }
            }
        } else if (splitByInitial(msg.getName()) == 1){
            try {
                scala.concurrent.Future<Object> waitingForGetMsg = ask(worker1, msg, 1000);
                scala.concurrent.duration.Duration timeout = scala.concurrent.duration.Duration.create(1, SECONDS);

                Object res = waitingForGetMsg.result(timeout, null);
                ReplyMsg replyMsg = (ReplyMsg) res;
                getSender().tell(replyMsg, getSelf());

            } catch (TimeoutException teWorker1) {
                System.out.println("BALANCER: Primary copy query for name " + msg.getName() + " is resting!");

                try {
                    scala.concurrent.Future<Object> waitingForGetMsg1 = ask(worker0, msg, 1000);
                    scala.concurrent.duration.Duration timeout = scala.concurrent.duration.Duration.create(1, SECONDS);
                    Object res2 = waitingForGetMsg1.result(timeout, null);
                    ReplyMsg replyMsg2 = (ReplyMsg) res2;
                    getSender().tell(replyMsg2, getSelf());

                } catch (TimeoutException teWorker0) {
                    System.out.println("BALANCER: Both copies are resting for name " + msg.getName() + "!");
                    getSender().tell(new TimeoutMsg(), getSelf());
                }
            }

        }
	}

	void storeEntry(PutMsg msg) {
		System.out.println("BALANCER: Received new entry " + msg.getName() + " - " + msg.getEmail());
        if (splitByInitial(msg.getName()) == 0){
            worker0.tell(new WorkerMsg(msg, false), self());
            worker1.tell(new WorkerMsg(msg, true), self());
        }
        else{
            worker0.tell(new WorkerMsg(msg, true), self());
            worker1.tell(new WorkerMsg(msg, false), self());
        }
	}

	static Props props() {
		return Props.create(AddressBookBalancerActor.class);
	}

}
