package org.learning24.java8.FunctionalInterfaces;

import org.learning24.java8.data.Student;
import org.learning24.java8.data.StudentDataBase;

import java.util.List;
import java.util.function.BiConsumer;

public class BiConsumerExample {

    public static void printStudentAndActivities(){

        List<Student> students = StudentDataBase.getAllStudents();

        BiConsumer<String, List<String>> studentStudentBiConsumer = (name, activities) -> {
            System.out.println(name + ": "+ activities);
        };

        students.forEach(
                (student -> studentStudentBiConsumer.accept(student.getName(), student.getActivities()))
        );

    }



    public static void main(String[] args) {
        BiConsumer<String, String> biConsumer = (a,b) -> {
            System.out.println("a: "+a+" b: "+b);
        };
        biConsumer.accept("java8", "learning");


        /**
         * Multiplication and division of two numbers using andThen method
         */

        BiConsumer<Integer, Integer> integerMultiplication = (a,b) ->
            System.out.println("Multiplication: "+(a * b));

        BiConsumer<Integer, Integer> integerDivision = (a,b) ->
                System.out.println("Division: "+(a / b));

        integerMultiplication.andThen(integerDivision).accept(4,2);

        printStudentAndActivities();

    }
}