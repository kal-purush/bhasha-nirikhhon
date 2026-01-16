package com.saikat;

import java.util.Scanner;

public class ProductofDigits {
    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);
        int n = sc.nextInt();
        int p = 1;
        int ans = prodOfDig(n,p);
        System.out.print("Product of digits is : "+ans);
    }
    static int prodOfDig(int n, int prod)
    {
        if (n<1)
            return prod;
        prod*=n%10;
        return prodOfDig(n/10,prod);
    }
}