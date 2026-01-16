package learn.ijpds2nd.ch10oop;

import learn.ijpds2nd.tools.ArrayIterator;

import java.util.Arrays;
import java.util.Iterator;

/* Listing 10.6 */
public class Course implements Iterable<String> {
    private final String name;
    private final String[] students = new String[100];
    private int numberOfStudents;

    public Course(String name) {
        this.name = name;
    }

    public void addStudent(String student) {
        students[numberOfStudents] = student;
        numberOfStudents++;
    }

    public String getName() {
        return name;
    }

    public int getNumberOfStudents() {
        return numberOfStudents;
    }

    public String[] getStudents() {
        return Arrays.copyOf(students, numberOfStudents);
    }

    @SuppressWarnings("EmptyMethod")
    public void dropStudent(String student) {
        // left as an exercise
    }

    @Override
    public Iterator<String> iterator() {
        return new ArrayIterator<>(students, numberOfStudents);
    }
}