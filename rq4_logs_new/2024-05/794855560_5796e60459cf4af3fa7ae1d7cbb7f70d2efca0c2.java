package org.learning24.java8.FunctionalInterfaces;

import org.learning24.java8.data.Student;
import org.learning24.java8.data.StudentDataBase;

import java.util.List;
import java.util.function.Predicate;

public class PredicateStudentExample {

    static Predicate<Student> studentPredicateGrade = (student) -> student.getGradeLevel() >=3;
    static Predicate<Student> studentPredicateGPA = (student) -> student.getGpa() >=3.9;

    /**
     * Return true when the student grade is >= 3
     */
    public static void filterStudentsByGrade(){
        List<Student> studentList = StudentDataBase.getAllStudents();

        studentList.forEach(
                (student) -> {
                   if (studentPredicateGrade.test(student)){
                       System.out.println(student);
                   }});
    }
    /**
     * Filter students based on GPA
     */
    public static void filterStudentsByGPA(){
        List<Student> studentList = StudentDataBase.getAllStudents();
        studentList.forEach(
                (student) -> {
                    if (studentPredicateGPA.test(student)){
                        System.out.println(student);
                    }});
    }

    /**
     *
     * Filter based on both the conditions
     */
    public static void filterBasedOnBothGPAAndGrade(){
        List<Student>studentList = StudentDataBase.getAllStudents();
        studentList.forEach(
                (student) -> {
                    if (studentPredicateGrade.and(studentPredicateGPA).test(student)){
                        System.out.println(student);
                    }
                }
        );
    }

    public static void main(String[] args) {
        filterStudentsByGrade();
        filterStudentsByGPA();
        filterBasedOnBothGPAAndGrade();
    }
}