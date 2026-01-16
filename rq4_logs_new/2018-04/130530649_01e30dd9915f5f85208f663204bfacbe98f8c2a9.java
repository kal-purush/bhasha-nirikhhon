package com.company;

public class Main {

    public static void main(String[] args) {
        float x1 = 1.e5f;
        float x2 = 1.e-5f;
        float x3 = x1 + x2;

        //переполнение
        int r = 128;
        //byte i = r;

        //принудительно
        byte i2 = (byte) r;

        System.out.println("x3 = " + x3);
        System.out.println(i2);
        if (x1 == x3) {
            System.out.println("Yes!");
        } else {
            System.out.println("No!");
        }

    }

}