package bgu.spl.a2.TestClass;

import bgu.spl.a2.sim.privateStates.CoursePrivateState;
import bgu.spl.a2.sim.privateStates.DepartmentPrivateState;
import bgu.spl.a2.sim.privateStates.StudentPrivateState;

import java.util.List;

public class Help {
    List Departments;
    List Courses;
    List Students;

    public Help(List<DepartmentPrivateState> department, List<CoursePrivateState> course, List<StudentPrivateState>students){
        Departments = department;
        Courses = course;
        Students = students;
    }

}