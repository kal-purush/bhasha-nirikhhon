package com.yearup;

import java.util.Scanner;

/**
 * This class generates the volunteer who should answer the quiz
 */
public class VolunteerPicker {
    Student[] Students;

    public static void main(String[] args) {
        int input;
        do{
            System.out.println("*********************************************************");
            System.out.println("Your turn will come too!!");
            System.out.println("1. Choose Volunteer\n2. Reset answered students\n3. Exit");
            Scanner sc = new Scanner(System.in);
            input = sc.nextInt();
            switch (input){
                case 1: chooseVolunteer();break;

//                case 2: resetAlreadyAnswered(); break;
            }
        }while(input!=3);
    }

    private static void chooseVolunteer() {

    }
}


class Student{
    private String firstName;
    private String lastName;
    private Integer score;
    private boolean alreadyAnswered;

    Student(String firstName, String lastName) {
        this.firstName = firstName;
        this.lastName = lastName;
    }

    public Integer getScore() {
        return score;
    }

    public void setScore(Integer score) {
        this.score = score;
    }
}