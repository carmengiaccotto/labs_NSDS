package com.Exercises.Lab25;

import akka.actor.ActorRef;

public class ConfigMsg {
    private final ActorRef RefWorker0;
    private final ActorRef RefWorker1;
    private final ActorRef Balancer;

    public ConfigMsg(ActorRef refWorker0, ActorRef refWorker1, ActorRef Balancer){
        this.RefWorker0 = refWorker0;
        this.RefWorker1 = refWorker1;
        this.Balancer = Balancer;
    }

    public ActorRef getRefWorker0() {
        return RefWorker0;
    }

    public ActorRef getRefWorker1() {
        return RefWorker1;
    }

    public ActorRef getBalancer() {
        return Balancer;
    }
}
