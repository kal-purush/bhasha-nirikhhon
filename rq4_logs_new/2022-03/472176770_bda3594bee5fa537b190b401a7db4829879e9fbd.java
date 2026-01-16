package triangle;

import java.util.Scanner;

public class Triangle {
    double a,b,c,p, CV, S;
    public Triangle() {
        Scanner sc = new Scanner(System.in);
        System.out.println("Nhap vao 3 canh a,b,c");
        a = sc.nextInt();
        b = sc.nextInt();
        c = sc.nextInt();
    }

    void chuvi() {
         CV = a + b + c;
        System.out.println("Chu vi tam giac la: " + CV);
    }

    void dienTich() {
        p = CV /2;
        S =  Math.sqrt(p*(p-a)*(p-b)*(p-c));
        System.out.println("Dien tich tam giac la: "+ S);
    }

}