package bgu.spl.a2.TestClass;

import bgu.spl.a2.Action;

import java.util.Collection;
import java.util.LinkedList;

public class ActionBasic extends Action{


//    public ActionBasic()
    @Override
    protected void start() {
        //Creates the list of actions I need to finish before doing my task
        Collection<? extends Action<?>> listOfActions = new LinkedList<>();
        addToListOfActions();

        if (!listOfActions.isEmpty())
            for(Action<?> action: listOfActions){
                sendMessage(action, actorId, actorState); // todo - where do we want to add the action??????
            }

        then(listOfActions, ()->{
            int result = 0;
            System.out.print("I love halav");
            complete(result);
        });
    }

    private void addToListOfActions(){}
}