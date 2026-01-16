public class Main {
    // Recursive method to calculate the nth Fibonacci term
    public static int calculateFibonacci(int n) {
        if (n <= 0) {
            return 0;
        } else if (n == 1) {
            return 1;
        } else {
            return calculateFibonacci(n - 1) + calculateFibonacci(n - 2);
        }
    }

    public static void main(String[] args) {
        int n = 10; // Change this value to calculate a different term in the sequence
        int nthTerm = calculateFibonacci(n);

        System.out.println("The " + n + "th term of the Fibonacci sequence is " + nthTerm + ".");
    }
}