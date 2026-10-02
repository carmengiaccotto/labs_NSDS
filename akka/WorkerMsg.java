package com.Exercises.Lab25;

public class WorkerMsg {
    private PutMsg putMsg;
    private boolean copy;

    public WorkerMsg(PutMsg putMsg, boolean copy){
        this.putMsg = putMsg;
        this.copy = copy;
    }
    public PutMsg getPutMsg() {
        return putMsg;
    }

    public boolean isCopy() {
        return copy;
    }
}
