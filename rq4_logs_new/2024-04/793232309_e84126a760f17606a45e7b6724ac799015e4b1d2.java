import java.util.Scanner;

public class Main {
    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        // Get the number of elements in the Fibonacci series from the user
        System.out.print("Eleman sayısını girin: ");
        int n = scanner.nextInt();

        int a = 0, b = 1;
        // Print the Fibonacci series to the screen
        System.out.println("Fibonacci Serisi:");
        for (int i = 0; i < n; i++) {
            System.out.print(a + " ");
            // Calculate the next Fibonacci number
            int sum = a + b;
            a = b;
            b = sum;
        }
    }
}