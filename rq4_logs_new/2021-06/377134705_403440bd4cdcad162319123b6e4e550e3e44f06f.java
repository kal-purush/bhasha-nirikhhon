package com.company;

import java.util.Scanner;


public class Main {

    public static void main(String[] args) {

        Scanner in = new Scanner(System.in);
        System.out.print("Enter name and last name: ");
        String name = in.nextLine();
        System.out.print("Enter phone number: ");
        String num = in.nextLine();
        System.out.print("Enter email: ");
        String mail = in.nextLine();

        boolean res1 = name.matches("^([A-Z][a-z]*((\\s)))+[A-Z][a-z]*$");
        if (res1) {
            System.out.println("Name validation successful");
        }
            else {
            System.out.println("Illegal. Name must contains only A-Z, a-z and space");
            }

        boolean res2 = num.matches("(\\+)\\d{12}");
        if (res2) {
            System.out.println("Phone number validation successful");
            }
            else {
                System.out.println("Illegal. Phone number must contains only numbers and +");
            }

        boolean res3 = mail.matches("^[A-Z0-9._%+-]+@[A-Z0-9.-]+\\.[A-Z]{2,6}$");
        if (res3) {
            System.out.println("Email validation successful");
        }
            else {
            System.out.println("Illegal. Email address must be in a format: (A-Z,0-9,_.+-)@(A-Z0-9.-).[A-Z]");
            }
        }

    }