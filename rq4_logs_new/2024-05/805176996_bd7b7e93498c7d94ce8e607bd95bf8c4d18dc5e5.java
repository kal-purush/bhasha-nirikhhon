package org.example.ClassAndObject;

public class Client {
    public static void main(String args[]){
        Student student=new Student();
        /*
        new student() => creates an object of student and returns the reference of the student class.
        the object contains the copy all the attrribute present inside the class.
         */
        student.rollNumber=1;
        student.name="Sai Teja";
        student.psp=94.5;
        Student student1=new Student();
        student1.rollNumber=2;
        student1.name="Abhilash";
        student1.psp=97.5;
        student1.batch_id=4;

        System.out.println(student);
        System.out.println(student1);

        student.chnageBatch(1);
        student.attendClass();

        student1.pauseCourse();
        student1=student;
        System.out.println(student);
        System.out.println(student1);
    }
}