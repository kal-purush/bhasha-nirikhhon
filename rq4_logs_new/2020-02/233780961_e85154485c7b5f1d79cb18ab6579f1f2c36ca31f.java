package ru.task5;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Group<T extends Number> {

    private Subject subject;
    private Map<Student, List<T>> marksList;

    public Group(Subject subject) {
        this.subject = subject;
        this.marksList = new HashMap<Student, List<T>>();
    }

    public Subject getSubject() {
        return subject;
    }

    public Map<Student, List<T>> getMarksList() {
        return marksList;
    }

    public void addStudentToGroup(Student student) {
        marksList.put(student, new ArrayList<>());
        student.addGroup(this);
    }

    public List<T> getStudentsMark(Student student) {
        return marksList.get(student);
    }

    public void addMarkToStudent(Student student, T mark) {

        if (marksList.get(student) == null) {
            throw new Error(student.getName() + " - нет в группе " + this.subject);
        }

        List<T> newListOfMarks = new ArrayList<T>();
        newListOfMarks.addAll(marksList.get(student));

        marksList.put(student, newListOfMarks);
    }
}