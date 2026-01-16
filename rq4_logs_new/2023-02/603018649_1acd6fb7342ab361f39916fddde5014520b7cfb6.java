package Rata_rata;
import java.util.Scanner;

public class Nilai_rata_rata {

	public static void main(String[] args) {
         Scanner input = new Scanner (System.in);
         double a,b,c;
         int x,y;
         
         System.out.println("======================");
         System.out.print("Masukkan Banyak Angka : "); y = input.nextInt();
         System.out.println("======================");
         System.out.println("");
         
         a = 0;
         x = 1;
         while (x <= y) {
        	 System.out.print("Angka Ke - "+x+ " : " ); b = input.nextDouble();
        	 a += b ;
        	 x++;
         }
         
         c = a / y;
         System.out.println("");
         System.out.println("======================");
         System.out.println("Nilai Rata Rata : "+c);
         System.out.println("======================");
         

	}

}