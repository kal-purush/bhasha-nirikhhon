package task5;

import java.util.Arrays;
import java.util.List;

public class Main {

    public static void main(String[] args) {
        Subject math = new Subject("math", false);
        Subject algebra = new Subject("algebra", false);
        Subject history = new Subject("history", true);

        Student student1 = new Student("alex");
        Student student2 = new Student("peter");
        Student student3 = new Student("oleg");
        Student student4 = new Student("vadim");
        Student student5 = new Student("sam");


        student1.setMark(math, 5);
        student1.setMark(algebra, 3.5);
        student1.setMark(history, 4);

        student2.setMark(history, 5);
        student2.setMark(math, 5);

        student3.setMark(math, 4.5);
        student3.setMark(math, 4.5);
        student3.setMark(math, 5);

        student4.setMark(history, 5);
        student4.setMark(algebra, 2);

        student5.setMark(history, 2.5);

        List<Student> studentsList = Arrays.asList(student1, student2, student3, student4, student5);

        //группа по математике
        studentsList.stream().filter(student -> student.getMarks().containsKey("math")).forEach(System.out::println);

        //оценки в группе по математике
        studentsList.stream().
                map(Student::getMarks).
                filter(subject -> subject.containsKey("math")).
                map(subject -> subject.get("math")).
                mapToDouble(i -> i).
                average().
                ifPresent(System.out::println);
    }
}