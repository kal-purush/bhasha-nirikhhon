package com.java.excercises.school;

import java.util.ArrayList;
import java.util.List;

public class SchoolManagement {

	
	private static List <Student> studentList= new ArrayList<>();
	
	private void addStudentToList(Student student){
		studentList.add(student);
		
	}
	private void showStudentList() {
	
	for(int i=0; i<studentList.size(); i++) {
		System.out.println(studentList.get(i));
	}
	
	}

public static void main (String[] args) {
	
	SchoolManagement schoolManagement = new SchoolManagement();
	Student studentObj1 = new Student(1, "Armando", "Sistemas", 3500);
	Student studentObj2 = new Student(2, "Mariana", "Sistemas", 1500);
	Student studentObj3 = new Student(3, "Perla", "Sistemas", 2500);
	
	schoolManagement.addStudentToList(studentObj1);
	schoolManagement.addStudentToList(studentObj2);
	schoolManagement.addStudentToList(studentObj3);
	
	schoolManagement.showStudentList();
}
}