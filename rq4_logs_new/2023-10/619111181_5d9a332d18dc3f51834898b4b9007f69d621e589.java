import basic.Laptop;
import basic.Student;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class Main {
    public static void main(String[] args) {
//        Student student = new Student(10, "pradeep", "mca");
//        System.out.println(student.id+" "+student.name+" "+student.course);
//        Laptop laptop = new Laptop("lenovo", "i7", "kill8");
//        System.out.println(laptop.toString()+ laptop);
        List<Student> list = new ArrayList<>();
        list.add(new Student(10, "Pradeep", "bca"));
        list.add(new Student(20, "sanjay", "mca"));
        list.add(new Student(31, "killer", "mba"));

//        list.sort(Comparator.comparing(Student::getId).reversed());
//        list.sort((data1, data2) -> data2.getId() - data1.getId());
        list.sort((data1, data2) -> Integer.compare(data2.id, data1.id));
        list.forEach((data) -> System.out.println(data.id+" "+data.name+" "+data.course));
    }
}