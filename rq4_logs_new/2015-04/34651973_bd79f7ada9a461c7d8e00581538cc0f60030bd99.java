package edu.iis.mto.multithread;

import java.util.concurrent.Executor;

public class BetterRadar {
    private Executor executor;
    private ReactionTask task;


    public BetterRadar(ReactionTask task, Executor executor) {
        this.task = task;
        this.executor = executor;
    }

    public void notice(Scud enemyMissile) {
        executor.execute(task);
    }

}