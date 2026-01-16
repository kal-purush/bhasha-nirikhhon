package com;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

public class DataManager {

	private Map<String, Subject> subjects;
	private Map<Integer, Student> students;
	private int totalStudentsTakeMoreThanOneClass;
	
	public DataManager() {
		subjects = new HashMap<String, Subject>();
		students = new HashMap<Integer, Student>();
		totalStudentsTakeMoreThanOneClass = 0;
		
		for (SubjectTitle title : SubjectTitle.values()) {
			subjects.put(title.name(), new Subject(title.name()));
		}
	}
	
	public void LoadData(String filePath) {
		
		totalStudentsTakeMoreThanOneClass = 0;
		Scanner data = null;
		
		try {
			data = new Scanner(new File(filePath));
			while (data.hasNext()) {
				String row = data.nextLine().trim();
				processData(row);
			}
		} catch (Exception e) {
			// log exception to file
			//e.printStackTrace();
		} finally {
			data.close();
		}
	}
	
	public int getTotalStudentsTakeMoreThanOneClass() {
		return totalStudentsTakeMoreThanOneClass;
	}
	
	public int getTotalStudentinClass( SubjectTitle title) {
		Subject subject = subjects.get(title.name());
		return subject.getTotalStudentEnroll();
	}
	
	private void processData(String data) {
		
		String[] fields = data.split(",");
		
		for (int i=0; i<fields.length; i++) {
			fields[i] = fields[i].trim();
		}
		
		if (validateFields(fields)) {
			this.enrollStudentAndProfessorInClass(Integer.parseInt(fields[2]), fields[1], fields[0]);
		}

	}
	
	private Boolean validateFields(String[] fields) {
		if (fields.length != 3) { 
			// log incomplete dataSet
			return false; 
		}
		
		if (fields[0].isEmpty()) {
			// log missing Subject
			return false;
		}
		
		if (!subjects.containsKey(fields[0])) {
			// log incorrect Subject title
			return false;
		}
		
		if (fields[1].isEmpty()) {
			// log missing Professor
			return false;
		}
		
		if (fields[2].isEmpty()) {
			// log missing Student ID
			return false;
		}   
		
		char[] chrs = fields[2].toCharArray();
        for (char chr1 : chrs) {
            if (chr1 < 49 || chr1 > 57) {
            	// log Student ID is not a number
            	return false;
            }
        }
        
        return true;
	}
	
	private Boolean isStudentExist(int id) {
		return students.containsKey(id);
	}
	
	private void addNewStudent(int id) {
		Student newStudent = new Student(id);
		students.put(id, newStudent);
	}
	
	private void enrollStudentAndProfessorInClass(int StudentID, String professor, String subject) {
		if (!isStudentExist(StudentID)) {
			addNewStudent(StudentID);
		}
		
		Student student = students.get(StudentID);
		if (!student.isEnrollInClass(SubjectTitle.valueOf(subject))) {
			student.enrollIn(SubjectTitle.valueOf(subject));
			if (student.totalClassEnroll() == 2) totalStudentsTakeMoreThanOneClass++;
		}
		
		Subject enrollClass = subjects.get(subject);
		enrollClass.addProfessor(professor);
		enrollClass.addStudent(StudentID);
	}
	
}