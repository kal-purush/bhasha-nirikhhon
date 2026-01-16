package CodeforcesRaif.A;

import java.util.Scanner;

public class Program {
    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);
        int tc = sc.nextInt();
        while (tc-- > 0) {
            int x1 = sc.nextInt();
            int y1 = sc.nextInt();
            int x2 = sc.nextInt();
            int y2 = sc.nextInt();
            if (x1 == x2) {
                System.out.println(Math.abs(y1 - y2));
            } else if (y1 == y2) {
                System.out.println(Math.abs(x1 - x2));
            } else {
                System.out.println(Math.abs(x1 - x2) + 2 + Math.abs(y1 - y2));
            }
        }
    }
}