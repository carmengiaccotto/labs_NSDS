package com.Exercises.Lab25;
import java.util.HashMap;

import akka.actor.AbstractActor;
import akka.actor.Props;

public class AddressBookWorkerActor extends AbstractActor {

	private HashMap<String, String> primaryAddresses;
	private HashMap<String, String> replicaAddresses;

	public AddressBookWorkerActor() {
		this.primaryAddresses = new HashMap<String, String>();
		this.replicaAddresses = new HashMap<String, String>();
	}

	@Override
	public Receive createReceive() {
		return awake();
	}

    public Receive awake() {
        return receiveBuilder()
                .match(WorkerMsg.class, this::onWorkerMsg)
                .match(GetMsg.class, this::generateReply)
                .match(RestMsg.class, this::onRestMsg)
                .build();
    }

    public Receive sleep() {
        return receiveBuilder()
                .match(WorkerMsg.class, this::onWorkerMsgSleep)
                .match(GetMsg.class, this::onGetMsgSleep)
                .match(RestMsg.class, this::onRestMsgSleep)
                .match(ResumeMsg.class, this::onResumeMsg)
                .build();
    }

    private void onWorkerMsgSleep(WorkerMsg msg) {}

    private void onGetMsgSleep(GetMsg msg) {}

    private void onRestMsgSleep(RestMsg msg) {}

    private void onResumeMsg(ResumeMsg msg) {
        getContext().become(awake());
    }

    private void onWorkerMsg(WorkerMsg msg) {
        if(!msg.isCopy()) {
            primaryAddresses.put(msg.getPutMsg().getName(), msg.getPutMsg().getEmail());
        }
        else{
            replicaAddresses.put(msg.getPutMsg().getName(), msg.getPutMsg().getEmail());
        }
    }

    private void onRestMsg(RestMsg msg) {
        getContext().become(sleep());
    }
	
	void generateReply(GetMsg msg) {
		System.out.println(this.toString() + ": Received query for name " + msg.getName());
        String email = primaryAddresses.get(msg.getName());
        if (email == null) {
            email = replicaAddresses.get(msg.getName());
        }
        getSender().tell(new ReplyMsg(email), getSelf());
	}

	static Props props() {
		return Props.create(AddressBookWorkerActor.class);
	}
}
