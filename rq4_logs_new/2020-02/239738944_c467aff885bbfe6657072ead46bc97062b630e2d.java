import java.util.ArrayList;

public class Teacher {

    private final String name;
    private ArrayList<Student> studentList;

    public Teacher(String name, ArrayList<Student> list) {
        this.name = name;
        this.studentList = list;
    }

    public String getName() {
        return name;
    }

    public ArrayList<Student> getStudentList() {
        return studentList;
    }

    public void setStudentList(ArrayList<Student> studentList) {
        this.studentList = studentList;
    }
}