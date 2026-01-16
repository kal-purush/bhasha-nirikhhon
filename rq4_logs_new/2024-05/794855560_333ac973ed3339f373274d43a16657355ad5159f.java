package org.learning24.java8.FunctionalInterfaces;

import org.learning24.java8.data.Student;
import org.learning24.java8.data.StudentDataBase;

import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;

public class ConsumerPlusPredicateExample {

    /**
     *
     * creating an instance method instead of static to show lambda works the same in both the cases.
     */

    Predicate<Student> p1 = (student) ->
        student.getGradeLevel() >=3;
    Predicate<Student> p2 = (student) -> student.getGpa() >= 3.9;
    BiConsumer<String, List<String>> studentListBiConsumer = (name, activities) -> {
        System.out.println("name: "+name+ " Activities: "+activities);
    };
    Consumer<Student> studentConsumer = (student) -> {
        if (p1.and(p2).test(student)){
            studentListBiConsumer.accept(student.getName(), student.getActivities());
        }
    };

    public void printNameAndActivities(){
        List<Student> students = StudentDataBase.getAllStudents();
        students.forEach(studentConsumer);
    }
    public static void main(String[] args) {
        new ConsumerPlusPredicateExample().printNameAndActivities();
    }
}