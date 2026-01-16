package CIE;

import java.util.Scanner;

public class internals {
        public void acceptmark() {
            double t = 0;
            int j;
            Scanner sc = new Scanner(System.in);
            double[] marks;
            marks=new double[6];
                for ( j = 1; j <= 5; j++) {
                    System.out.println("Enter the CIE marks of Subject "+j+" :\t");
                    marks[j] = sc.nextInt();
                }
            for ( int i = 1; i <= 5; i++) {
                t = t + marks[i];
            }
              System.out.println("Total marks of is CIE is:\t"+t);
            }
}