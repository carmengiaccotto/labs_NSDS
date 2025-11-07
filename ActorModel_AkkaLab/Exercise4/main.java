package com.Exercises.Ex4;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

public class main {
    public static void main(String[] args) throws Exception {
        ActorSystem system = ActorSystem.create("Exercise4System");

        ActorRef server = system.actorOf(ServerActor.props(), "server");
        ActorRef client = system.actorOf(Actor.props(), "client");

        server.tell(new Text("one"), client);
        server.tell(new Sleep(), client);
        server.tell(new Text("two"), client);
        server.tell(new Text("three"), client);
        server.tell(new Text("four"), client);
        server.tell(new WakeUp(), client);
        server.tell(new Text("five"), client);


    }
}