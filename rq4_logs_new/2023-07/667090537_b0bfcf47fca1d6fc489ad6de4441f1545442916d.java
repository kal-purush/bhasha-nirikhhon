import java.util.Scanner;

public class MultiplicationTable {
    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        int amountColumns = scanner.nextInt();
        int amountLines = scanner.nextInt();
        for (int i = 1; i <= amountColumns; i++) {
            for (int k = 1; k <= amountLines; k++) {
                if (i == amountLines) {
                    System.out.println(k*i);
                    System.out.println();
                    return;
                }
                System.out.print(k*i + " ");
            }
            System.out.println();
        }
    }
}