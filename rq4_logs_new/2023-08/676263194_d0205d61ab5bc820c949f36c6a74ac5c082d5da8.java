package main;

import models.Student;
import models.Doctor;
import models.Worker;

import java.util.Scanner;

public class Main {
    static Scanner reader = new Scanner(System.in);

    public static void main(String[] args) {
        System.out.println("Enter Your Job");
        String yourJob = reader.nextLine();
        switch (yourJob) {
            case ("student"):
                Student student = new Student("Mustafa", 29, "Engineer");
                System.out.println(student.getStudentData());
                break;

            case ("doctor"):
                Doctor doctor = new Doctor("Ali", 35, 750);
                System.out.println(doctor.getDoctorData());
                break;
            case ("worker"):
                Worker worker = new Worker("Ahmed", 45, "A Builder");
                System.out.println(worker.getWorkerData());
                break;
            default:
                System.out.println("Invalid ! Re Enter Your Job");
                reader.nextLine();

                while (!(yourJob.equals("student") || yourJob.equals("doctor") || yourJob.equals("worker"))) {
                    System.out.println("Invalid ! Re Enter Your Job");
                    reader.nextLine();

                }



        }

    }
}