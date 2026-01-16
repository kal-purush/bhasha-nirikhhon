import java.util.Scanner;
public class Nomor1 {

    public static void main(String[] args) {
        Scanner iu = new Scanner(System.in);
        System.out.println("input bilangan pertama :");
        int a = iu.nextInt();
        System.out.println("input bilangan kedua   :");
        int b = iu.nextInt();
        System.out.println("fpb dari " + a + " dan " + b + " = " + carifpb(a,b));

    }
    public static int carifpb(int angka1,int angka2) {
        int hasil = angka1 % angka2;
        while (hasil !=0) {
            angka1 = angka2;
            angka2 = hasil;
            hasil  = angka1 % angka2; 
        }
        return angka2;

    } 

}