import java.util.Scanner;

class Nomor1 {
    public static void main(String[] args) {
        Scanner obj = new Scanner(System.in);
        System.out.printf("Masukkan x :");
        int x = obj.nextInt();
        System.out.printf("Masukkan y :");
        int y = obj.nextInt();
        for (int i = x < y ? x : y; i <= (x > y ? x : y); i++) {
            if (i % 2 == 0 && i > 0) {
                System.out.println(i + " adalah genap positif");
            } else if (i % 2 == 0 && i < 0) {
                System.out.println(i + " adalah genap negatif");
            } else if (i == 0) {
                System.out.println(i + " nol");
            } else if (i < 0) {
                System.out.println(i + " adalah ganjil negatif");
            } else {
                System.out.println(i + " adalah ganjil positif");
            }
        }
    }
}