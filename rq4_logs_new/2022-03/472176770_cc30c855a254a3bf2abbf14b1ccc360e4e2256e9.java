package com.company;

import java.util.Scanner;

public class Fibonaci {
    //2. Nhập 1 số nguyên n, tìm số có vị trí thứ n của dãy Fibonaci.
        int fibo(int n) {
            if (n==1 || n==2) return 1;
            int i=3, a=0, a1=1, a2=1;
            while (i<=n){
                a= a1+a2;
                a1 = a2;
                a2 = a;
                i++;
            }
            return a;
        }
    public static void main(String[] args){
        Scanner sc = new Scanner(System.in);
        System.out.println("Enter Number: ");
        int n = sc.nextInt();
        Fibonaci fb = new Fibonaci();
        System.out.println(n + "th Fibonaci is: " +fb.fibo(n));
    }
}