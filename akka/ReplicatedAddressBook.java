package com.Exercises.Lab25;
import java.util.concurrent.TimeUnit;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

public class ReplicatedAddressBook {

	public static void main(String[] args) {

		// Timeouts are used to ensure message queues are flushed
		final int timeStep = 2;

		final ActorSystem sys = ActorSystem.create("System");
        final ActorRef balancer = sys.actorOf(AddressBookBalancerActor.props(), "balancer");
        final ActorRef worker0  = sys.actorOf(AddressBookWorkerActor.props(), "worker0");
        final ActorRef worker1  = sys.actorOf(AddressBookWorkerActor.props(), "worker1");
        final ActorRef client   = sys.actorOf(AddressBookClientActor.props(), "client");

        client.tell(new ConfigMsg(null, null, balancer), ActorRef.noSender());
        balancer.tell(new ConfigMsg(worker0, worker1, null), ActorRef.noSender());
		
		// Waiting until system is ready
		try {
			TimeUnit.SECONDS.sleep(timeStep);
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		// Populate some entries
		client.tell(new PutMsg("A", "a.b@polimi.it"), ActorRef.noSender());
		client.tell(new PutMsg("S", "s.z@polimi.it"), ActorRef.noSender());

		// Waiting until system is ready
		try {
			TimeUnit.SECONDS.sleep(timeStep);
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		// Return a.b@polimi.it from worker0 at the client
		client.tell(new GetMsg("A"), ActorRef.noSender());

		// Waiting until system is ready
		try {
			TimeUnit.SECONDS.sleep(timeStep);
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		// Make worker0 rest
		worker0.tell(new RestMsg(), ActorRef.noSender());

		// Waiting until system is ready
		try {
			TimeUnit.SECONDS.sleep(timeStep);
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		// Now return a.b@polimi.it from worker1 at the client, worker1
		// has the secondary copy
		client.tell(new GetMsg("A"), ActorRef.noSender());

		// Waiting until system is ready
		try {
			TimeUnit.SECONDS.sleep(timeStep);
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		// Make worker1 rest also
		worker1.tell(new RestMsg(), ActorRef.noSender());

		// Waiting until system is ready
		try {
			TimeUnit.SECONDS.sleep(timeStep);
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	
		// No copy is available; the client receives a TimeoutMsg as a Reply!
		client.tell(new GetMsg("A"), ActorRef.noSender());
		
		// Waiting until system is ready
		try {
			TimeUnit.SECONDS.sleep(timeStep*10);
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		sys.terminate();

	}

}
