package com.company.ksl.y2018.feb.fourthweek;

//feb27-2018
//https://vjudge.net/contest/214014#problem/A

import java.util.Scanner;

public class Tue {
    public static void main(String [] args){
        Scanner sc = new Scanner(System.in);
        while (sc.hasNextLong()) {
            long a = sc.nextLong(), b = sc.nextLong();
            System.out.println((a*b)/2);
        }
    }
}