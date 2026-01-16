package bgu.spl.a2.TestClass;

import bgu.spl.a2.Action;

import java.util.Collection;
import java.util.LinkedList;

public class ActionExtended extends Action{


    protected synchronized void start() {
        //Creates the list of actions I need to finish before doing my task
        LinkedList<Action> listOfActions = new LinkedList<>();

        listOfActions.add(new ActionBasic());
        listOfActions.add(new ActionBasic());

        if (!listOfActions.isEmpty())
            for(Action action: listOfActions){
                sendMessage(action, actorId, actorState); // todo - where do we want to add the action??????
            }

        then(listOfActions, ()->{
            int result = 0;
            System.out.println("I love cake");
            complete(result);
        });
    }
}