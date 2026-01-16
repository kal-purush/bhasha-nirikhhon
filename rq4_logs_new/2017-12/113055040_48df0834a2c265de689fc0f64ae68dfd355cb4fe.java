package bgu.spl.a2.sim.actions.OpenNewCourseActions;

import bgu.spl.a2.Action;
import bgu.spl.a2.sim.privateStates.CoursePrivateState;

import java.util.LinkedList;

//This action is in the Course actor
public class SetCourseInitials extends Action {
   private int availableSpots;
   private LinkedList<String> prerequisits;

    public SetCourseInitials(int availableSpots, LinkedList<String> prerequisits){
       this.availableSpots = availableSpots;
       this.prerequisits = prerequisits;
   }
    @Override
    protected void start() {
        then(new LinkedList<>(), ()->{
            ((CoursePrivateState)actorState).setAvailableSpots(availableSpots);
            ((CoursePrivateState)actorState).setPrequisites(prerequisits);
            complete(0);
        });
    }
}