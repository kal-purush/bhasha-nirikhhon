package com.gmail.xapchenko2000;

import java.util.Scanner;

public class Main {

    public static void main(String[] args) {

        passport();
        calculate();

    }


    public static void passport() {

        String name;
        String surname;
        int age;
        int birth;
        String cityBorn;
        String cCity;
        String sex;

        Scanner csOne = new Scanner(System.in);
        Scanner csTwo = new Scanner(System.in);
        Scanner sexIN = new Scanner(System.in);

        System.out.println("Enter your name: ");
        name = csOne.nextLine();


        System.out.println("Enter your surname: ");
        surname = csOne.nextLine();

        System.out.println("Enter your city where you were born: ");
        cityBorn = csOne.nextLine();

        System.out.println("Enter your age: ");
        age = csOne.nextInt();


        System.out.println("Enter your current city: ");
        cCity = csTwo.nextLine();

        System.out.println("Enter your sex: ");
        sex = sexIN.nextLine();


        System.out.println("\tPassport");
        System.out.println();
        System.out.print("Person:  " + name + " " + surname);
        System.out.println();
        System.out.println("Gender: " + sex + " age: " + age);
        System.out.println();
        System.out.println("Location: from - " + cityBorn + " " + "Current city -  " + cCity);


    }


    public static void calculate() {

        int a, b;

        Scanner number = new Scanner(System.in);

        System.out.print("Enter first number: ");
        a = number.nextInt();
        System.out.print("Enter second number: ");
        b = number.nextInt();

        System.out.println("1. " + a + "+" + b + " " + (a + b));
        System.out.println("2. " + a + "*" + b + " " + (a * b));
        System.out.println("3. " + a + "/" + b + " " + (a / b));
        System.out.println("4. " + a + "-" + b + " " + (a - b));
        System.out.println("5. " + a + "%" + b + " " + (a % b));
        System.out.println("6. " + a + "==" + b + " " + (a == b));
        System.out.println("7. " + a + ">" + b + " " + (a > b));
        System.out.println("8. " + a + "<" + b + " " + (a < b));

    }
}