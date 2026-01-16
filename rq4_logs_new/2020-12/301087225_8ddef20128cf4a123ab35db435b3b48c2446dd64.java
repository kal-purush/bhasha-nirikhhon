import java.util.Scanner;

public class Tugas1{
    public void carifpb(int a, int b) {
        int k = 2, l = 2;
        int x = a, y = b;
        while (x != y) {
            if (x > y) {
                while ((a % k) != 0) {
                    k++;
                }
                x = a / k;
                k++;
            } else {
                while ((b % l) != 0) {
                    l++;
                }
                y = b / l;
                l++;
            }
        }
        System.out.println("FPB dari " +a+ " dan " +b+ " = " + x);
    }
        
    public static void main(String[] args){
        Scanner obj = new Scanner(System.in);
        int a = obj.nextInt();
        int b = obj.nextInt();
        Tugas1 ab = new Tugas1();
        ab.carifpb(a, b);
    }
}