package day03nonprimivitesscannerclass;

import java.time.LocalDate;
import java.util.Scanner;

public class deneme {

    public static void main(String[] args) {

//        Scanner input = new Scanner(System.in);
//        System.out.println("give an odd num");
//        int a = input.nextInt();
//        String x= "*";
//        String y= " ";
//        int b= a/2;
//        for(int j=1;j<=a;j=j+2){
//
//            System.out.println(y.repeat(b)+x.repeat(j));
//            b--;
//        }

//        String x = "";
//        System.out.print(x);
//        for (int i=1;i<10;i++){
//            x=x+" "+i;
//            System.out.print(x);
//            System.out.println();
//        }


//        int i=13;
//        while (i<68){
//
//            System.out.print(i+" ");
//            i=i+2;
//        }

//        int i=12;
//        int sum = 0;
//        while (i<68){
//            sum=sum+i;
//            i++;
//        }
//        System.out.println(sum);


       int x = 1234;
       int sum = 0;
       while (x>0){
           sum = sum+x%10;
           x=x/10;
       }
        System.out.println(sum);





    }

}