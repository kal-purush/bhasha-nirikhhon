package com.example.multithreadingProject.StudentBuilder;

import com.example.multithreadingProject.StudentBuilder.model.Student;
import com.example.multithreadingProject.StudentBuilder.model.StudentBuilder;

public class StudentBuilderApplication {
    public static void main(String[] args) {
        Student student = new StudentBuilder(1).setFatherName("Father").build();
        System.out.println(student.toString());

        Student student1 = new StudentBuilder(2).setName("abc").setMotherName("efg").setFatherName("ijk").build();
        System.out.println(student1.toString());
    }
}