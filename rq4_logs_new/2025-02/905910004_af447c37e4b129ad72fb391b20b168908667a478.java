package lesson_15_Map_Interface;

import java.util.Comparator;
import java.util.TreeMap;

public class TreeMap_Example {
    public static void main(String[] args) {
        TreeMap<Double, Student> treeMap = new TreeMap<>();

        Student s1 = new Student("Zaur", "Tregulov", 3);
        Student s2 = new Student("Mariya", "Ivanova", 1);
        Student s3 = new Student("Sergey", "Petrov", 4);
        Student s4 = new Student("Igor", "Sidorov", 2);
        Student s5 = new Student("Vasyl", "Smirnov", 1);
        Student s6 = new Student("Sasha", "Kapustin", 3);
        Student s7 = new Student("Elena", "Sidorova", 4);

        treeMap.put(5.8,s1);
        treeMap.put(6.4,s2);
        treeMap.put(7.2,s3);
        treeMap.put(7.5,s4);
        treeMap.put(7.9,s5);
        treeMap.put(8.2,s6);
        treeMap.put(9.1,s7);

//        treeMap.put(s1, 5.8);
//        treeMap.put(s7, 9.1);
//        treeMap.put(s2, 6.4);
//        treeMap.put(s4, 7.5);
//        treeMap.put(s3, 7.2);
//        treeMap.put(s6, 8.2);
//        treeMap.put(s5, 7.9);
//        Student s8 = new Student("Elena", "Sidorova", 4);
//        System.out.println(treeMap.containsKey(s8));
//        System.out.println(treeMap);
//        System.out.println(treeMap.get(6.4));
//        treeMap.remove(5.8);
//        System.out.println(treeMap);


//        System.out.println(treeMap.descendingMap());
//        System.out.println(treeMap.tailMap(7.3));
//        System.out.println(treeMap.headMap(7.3));
//        System.out.println(treeMap.lastEntry());
//        System.out.println(treeMap.firstEntry());

    }
}

final class Student implements Comparable<Student> {
    private final String name;
    private final String surname;
    private final int course;

    public Student(String name, String surname, int course) {
        this.name = name;
        this.surname = surname;
        this.course = course;
    }

    @Override
    public boolean equals(Object student) {
        if (student == this) {
            return true;
        }
        if (student == null) {
            return false;
        }
        if (student.getClass().equals(Student.class)) {
            Student current = (Student) student;
            return current.name.equals(this.name)
                    && current.surname.equals(this.surname)
                    && current.course == this.course;
        }
        return false;
    }

    @Override
    public int hashCode(){
        int hashCode = 17;
        hashCode = 31 * hashCode + (name != null ? name.hashCode() : 0);
        hashCode = 31 * hashCode + (surname != null ? surname.hashCode() : 0);
        hashCode = 31 * hashCode + course;
        return hashCode;
    }

    @Override
    public String toString() {
        return "Student{" +
                "name='" + name + '\'' +
                ", surname='" + surname + '\'' +
                ", course=" + course +
                '}';
    }

    @Override
    public int compareTo(Student o) {
        return this.name.compareTo(o.name);
    }
}