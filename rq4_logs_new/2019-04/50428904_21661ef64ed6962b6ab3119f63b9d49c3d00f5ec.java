package com.company;

import java.util.Scanner;

public class Main {

    public static void main(String[] args) {


        Scanner scanner = new Scanner(System.in);


        int n = Integer.parseInt(scanner.nextLine());

        double p1 = 0;
        double p2 = 0;
        double p3 = 0;
        double p4 = 0;
        double p5 = 0;

        for (int i = 0; i <n ; i++) {

            int number = Integer.parseInt(scanner.nextLine());

            if (number >=1 && number < 200){
                p1 ++;
            } else if (number >=200 && number < 400){
                p2 ++;
            } else if (number >=400 && number < 600){
                p3 ++;
            } else if (number >=600 && number < 800){
                p4 ++;
            } else if (number >=800 && number <= 1000){
                p5 ++;
            }

        }

        System.out.printf("%.2f%%%n", p1 / n * 100);
        System.out.printf("%.2f%%%n", p2 / n * 100);
        System.out.printf("%.2f%%%n", p3 / n * 100);
        System.out.printf("%.2f%%%n", p4 / n * 100);
        System.out.printf("%.2f%%%n", p5 / n * 100);






    }
}