package com.Exercises.Ex4;

import akka.actor.AbstractActorWithStash;
import akka.actor.Props;

public class ServerActor extends AbstractActorWithStash {

    @Override
    public Receive createReceive() {
            return awake();
    }

    private Receive awake() {
        return receiveBuilder()
               .match(Text.class, this::onTextAwake)
               .match(Sleep.class, this::onSleep)
               .build();
    }

    private Receive sleeping() {
        return receiveBuilder()
                .match(Text.class, this::onTextSleeping)
                .match(WakeUp.class, this::onWakeup)
                .match(Sleep.class, this::onSleepWhileSleeping)
                .build();
    }

    private void onTextAwake(Text t) {
        getSender().tell(t, getSelf());
        System.out.println("[SERVER] echo: " + t.getText());
    }

    private void onSleep(Sleep s) {
        getContext().become(sleeping());
        System.out.println("[SERVER] going SLEEP");
    }

    private void onTextSleeping(Text t) {
        stash();
        System.out.println("[SERVER] stash: " + t.getText());
    }

    private void onWakeup(WakeUp w) {
        getContext().unbecome();
        System.out.println("[SERVER] WAKEUP, flushing stashed");
        unstashAll();
    }

    private void onSleepWhileSleeping(Sleep s) {
        System.out.println("[SERVER] already sleeping (noop)");
    }

    public static Props props() {
        return Props.create(ServerActor.class);
    }
}

