package devandroid.maddo.applistacurso.controller;

import java.util.ArrayList;
import java.util.List;

import devandroid.maddo.applistacurso.model.Course;

public class CourseController {
    public List courseList;

    public List getListaDeCursos(){
        courseList = new ArrayList<Course>();

        courseList.add(new Course());
        courseList.add(new Course());
        courseList.add(new Course());
        courseList.add(new Course());

        return courseList;
    }
}