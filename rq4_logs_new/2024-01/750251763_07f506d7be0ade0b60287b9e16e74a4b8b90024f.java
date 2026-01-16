import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.InputMismatchException;

class Student {
    private String id;
    private String name;
    private int gradeLevel; 
    private Map<String, Double[]> grades;
    private double totalGrade;
    private int rank;

    public Student(String id, String name, int gradeLevel) { 
        this.id = id;
        this.name = name;
        this.gradeLevel = gradeLevel;
        this.grades = new HashMap<>();
        this.totalGrade = 0;
        this.rank = 0;
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public int getGradeLevel() {
        return gradeLevel;
    }

    public Map<String, Double[]> getGrades() {
        return grades;
    }

    public void addGrade(String course, double midterm, double assignment, double finalExam) {
        grades.put(course, new Double[]{midterm, assignment, finalExam});
    }

    public void calculateTotalGrade() {
        totalGrade = 0;
        for (Double[] gradeArr : grades.values()) {
            for (double grade : gradeArr) {
                totalGrade += grade;
            }
        }
    }

    public double getTotalGrade() {
        return totalGrade;
    }

    public void setRank(int rank) {
        this.rank = rank;
    }

    public int getRank() {
        return rank;
    }
}

public class HighSchoolManagementSystem {
    private static Map<String, Student> students = new HashMap<>();
    private static Scanner scanner = new Scanner(System.in);

    public static void main(String[] args) {
        while (true) {
            displayMainMenu();
            int choice = scanner.nextInt();
            scanner.nextLine(); 
            switch (choice) {
                case 1:
                    studentInterface();
                    break;
                case 2:
                    teacherInterface();
                    break;
                case 3:
                    System.out.println("Exiting program...");
                    return;
                default:
                    System.out.println("Invalid choice. Please try again.");
            }
        }
    }

    private static void displayMainMenu() {
        System.out.println("\nMain Menu:");
        System.out.println("1. Student");
        System.out.println("2. Teacher");
        System.out.println("3. Exit");
        System.out.print("Enter your choice: ");
    }

    private static void studentInterface() {
        System.out.print("Enter your student ID: ");
        String studentId = scanner.nextLine();
        Student student = students.get(studentId);
        if (student != null) {
            System.out.println("Student Name: " + student.getName());
            Map<String, Double[]> grades = student.getGrades();
            System.out.println("Your Grades:");
            for (Map.Entry<String, Double[]> entry : grades.entrySet()) {
                Double[] gradeArr = entry.getValue();
                System.out.println(entry.getKey() + ": Midterm - " + gradeArr[0] + ", Assignment - " + gradeArr[1] + ", Final Exam - " + gradeArr[2]);
            }
            System.out.println("Your Total Grade: " + student.getTotalGrade());
            System.out.println("Your Rank: " + student.getRank());
        } else {
            System.out.println("Student not found with ID: " + studentId);
        }
    }

    private static void teacherInterface() {
        System.out.println("Teacher Interface:");
        System.out.println("1. Insert student grades");
        System.out.println("2. Update student grades");
        System.out.println("3. Delete student grades");
        System.out.println("4. Register new student");
        System.out.println("5. Back to Main Menu");
        System.out.print("Enter your choice: ");
        int choice = scanner.nextInt();
        scanner.nextLine(); 
        switch (choice) {
            case 1:
                insertGrades();
                break;
            case 2:
                updateGrades();
                break;
            case 3:
                deleteGrades();
                break;
            case 4:
                registerStudent();
                break;
            case 5:
                return;
            default:
                System.out.println("Invalid choice. Please try again.");
        }
    }

    private static void insertGrades() {
        System.out.print("Enter student ID: ");
        String studentId = scanner.nextLine();
        Student student = students.get(studentId);
        if (student != null) {
            System.out.println("Enter grades for each course:");
            for (String course : getCourses()) {
                System.out.println("Course: " + course);
                System.out.print("Midterm Grade (0-20): ");
                double midterm = validateGradeInput();
                System.out.print("Assignment Grade (0-30): ");
                double assignment = validateGradeInput();
                System.out.print("Final Exam Grade (0-50): ");
                double finalExam = validateGradeInput();
                student.addGrade(course, midterm, assignment, finalExam);
            }
            student.calculateTotalGrade();
            updateRank(); 
           
            saveToFile(student);
            System.out.println("Grades saved for student: " + student.getName());
        } else {
            System.out.println("Student not found with ID: " + studentId);
        }
    }

    private static void updateGrades() {
        /System.out.print("Enter student ID to update grades: ");
        String studentId = scanner.nextLine();
        Student student = students.get(studentId);
        if (student != null) {
            System.out.println("Enter updated grades for each course:");
            for (String course : getCourses()) {
                System.out.println("Course: " + course);
                System.out.print("Midterm Grade (0-20): ");
                double midterm = validateGradeInput();
                System.out.print("Assignment Grade (0-30): ");
                double assignment = validateGradeInput();
                System.out.print("Final Exam Grade (0-50): ");
                double finalExam = validateGradeInput();
                student.addGrade(course, midterm, assignment, finalExam);
            }
            student.calculateTotalGrade();
            updateRank();
           
            saveToFile(student);
            System.out.println("Grades updated and saved for student: " + student.getName());
        } else {
            System.out.println("Student not found with ID: " + studentId);
        }
    }

    private static void deleteGrades() {
        System.out.print("Enter student ID to delete grades: ");
        String studentId = scanner.nextLine();
        Student student = students.get(studentId);
        if (student != null) {
            student.getGrades().clear(); 
            student.calculateTotalGrade();
            updateRank();
            
            saveToFile(student);
            System.out.println("Grades deleted for student with ID: " + studentId);
        } else {
            System.out.println("Student not found with ID: " + studentId);
        }
    }

    private static void registerStudent() {
        System.out.print("Enter student ID: ");
        String studentId = scanner.nextLine();
        if (students.containsKey(studentId)) {
            System.out.println("Student with ID " + studentId + " already exists.");
            return;
        }
        System.out.print("Enter student name: ");
        String studentName = scanner.nextLine();
        int gradeLevel = askForGradeLevel(); 
        Student newStudent = new Student(studentId, studentName, gradeLevel); 
        students.put(studentId, newStudent);
        saveToFile(newStudent); 
        System.out.println("New student registered successfully.");
    }

    private static int askForGradeLevel() {
        int gradeLevel;
        while (true) {
            try {
                System.out.print("Enter student grade level (9, 10, 11, or 12): ");
                gradeLevel = scanner.nextInt();
                if (gradeLevel < 9 || gradeLevel > 12) {
                    throw new IllegalArgumentException("Grade level must be between 9 and 12.");
                }
                break;
            } catch (InputMismatchException e) {
                System.out.println("Invalid input. Please enter a valid grade level.");
                scanner.nextLine(); 
            } catch (IllegalArgumentException e) {
                System.out.println(e.getMessage());
            }
        }
        scanner.nextLine(); 
        return gradeLevel;
    }

    private static String[] getCourses() {
        return new String[]{"ENGLISH", "BIOLOGY", "CHEMISTRY", "PHYSICS", "MATH"};
    }

    private static double validateGradeInput() {
        double grade;
        while (true) {
            try {
                grade = scanner.nextDouble();
                if (grade < 0 || grade > 100) {
                    throw new IllegalArgumentException("Grade must be between 0 and 100.");
                }
                break;
            } catch (InputMismatchException e) {
                System.out.println("Invalid input. Please enter a valid grade.");
                scanner.nextLine(); 
            } catch (IllegalArgumentException e) {
                System.out.println(e.getMessage());
            }
        }
        return grade;
    }

    private static void updateRank() {
        List<Student> studentList = new ArrayList<>(students.values());
        Collections.sort(studentList, (s1, s2) -> Double.compare(s2.getTotalGrade(), s1.getTotalGrade()));
        for (int i = 0; i < studentList.size(); i++) {
            studentList.get(i).setRank(i + 1);
        }
    }

    private static void saveToFile(Student student) {
        try (FileWriter writer = new FileWriter(student.getName() + ".txt")) {
            Map<String, Double[]> grades = student.getGrades();
            writer.write("Student ID: " + student.getId() + "\n");
            writer.write("Student Name: " + student.getName() + "\n");
            writer.write("Grade Level: " + student.getGradeLevel() + "\n"); // Write grade level
            for (Map.Entry<String, Double[]> entry : grades.entrySet()) {
                Double[] gradeArr = entry.getValue();
                writer.write(entry.getKey() + ": Midterm - " + gradeArr[0] + ", Assignment - " + gradeArr[1] + ", Final Exam - " + gradeArr[2] + "\n");
            }
            writer.write("Total Grade: " + student.getTotalGrade() + "\n");
            System.out.println("Grades saved to file: " + student.getName() + ".txt");
        } catch (IOException e) {
            System.out.println("Error writing to file: " + e.getMessage());
        }
    }
}