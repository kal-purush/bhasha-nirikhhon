package bgu.spl.a2.sim.actions;

import bgu.spl.a2.Action;
import bgu.spl.a2.sim.privateStates.CoursePrivateState;
import bgu.spl.a2.sim.privateStates.DepartmentPrivateState;
import bgu.spl.a2.sim.privateStates.StudentPrivateState;

import java.util.LinkedList;

public class AddNewStudent extends Action{
    private DepartmentPrivateState myDepState;
    private String studentId;

    public AddNewStudent(String studentId, DepartmentPrivateState depState){
        actionName = "Add a new Student";
        this.studentId = studentId;
        myDepState = depState;
    }
    @Override
    protected void start() {
        LinkedList<Action> listOfActions = new LinkedList<>();

        then(listOfActions, ()->{
            myDepState.getStudentList().add(studentId);
            StudentPrivateState studentPrivateState = new StudentPrivateState();
            complete(0);
        });
    }
}