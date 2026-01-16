import java.util.*;

class Course {
    private String courseName;
    private int marks;

    public Course(String courseName, int marks) {
        this.courseName = courseName;
        this.marks = marks;
    }

    public String getCourseName() {
        return courseName;
    }

    public int getMarks() {
        return marks;
    }
}

class Student {
    private String name;
    private String program;
    private int semester;
    private List<Course> courses;

    public Student(String name, String program, int semester) {
        this.name = name;
        this.program = program;
        this.semester = semester;
        this.courses = new ArrayList<>();
    }

    public void registerCourse(String courseName, int marks) {
        courses.add(new Course(courseName, marks));
    }

    public void displayStudentDetails() {
        System.out.println("Name: " + name);
        System.out.println("Program: " + program);
        System.out.println("Semester: " + semester);
        System.out.println("Courses Registered:");
        for (Course c : courses) {
            System.out.println("- " + c.getCourseName());
        }
    }

    public void displayLowMarks() {
        System.out.println("Courses with marks less than 40:");
        for (Course c : courses) {
            if (c.getMarks() < 40) {
                System.out.println(c.getCourseName() + ": " + c.getMarks());
            }
        }
    }
}

public class Main {
    public static void main(String[] args) {
        Student student = new Student("Alice", "B.Sc. Computer Science", 3);
        student.registerCourse("Mathematics", 75);
        student.registerCourse("Data Structures", 35);
        student.registerCourse("Operating Systems", 28);

        student.displayStudentDetails();
        student.displayLowMarks();
    }
}