package com.Exercises.Ex4;

import akka.actor.AbstractActor;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;

public class Actor extends AbstractActor {

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Text.class, this::onText)   // method ref
                .build();
    }

    private void onText(Text t) {
        System.out.println("[CLIENT] received: " + t.getText());
    }

    public static Props props() {
        return Props.create(Actor.class);
    }
}