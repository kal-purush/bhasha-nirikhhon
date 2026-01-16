package bgu.spl.a2.sim.actions;

import bgu.spl.a2.Action;
import bgu.spl.a2.sim.privateStates.CoursePrivateState;

import java.util.LinkedList;

public class AddSpaces extends Action {
    private int num;

    public AddSpaces(int num){
        actionName = "Add Spaces";
        this.num = num;
        actorState = actorThreadPool.getPrivaetState(actorId);
    }
    @Override
    protected void start() {

        then(new LinkedList<>(), ()->{
            ((CoursePrivateState)actorState).setAvailableSpots(((CoursePrivateState)actorState).getAvailableSpots()+num);
            complete(0);
        });
    }
}