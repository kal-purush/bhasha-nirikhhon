package laboon;
public class Fibonacci {
	public static long fibonacci(long num) {
		if (num <= 1) {
			return num;
		} else {
			return fibonacci(num - 1) + fibonacci(num - 2);
		}
	}

	public static void main(String[] args) {
		if (args.length == 0) {
			System.out.println("Enter something buddy");
			System.exit(1);
		} else {
			try {
				long x = fibonacci(Long.parseLong(args[0]));
				System.out.println("Fibonacci is: " + x);
			} catch (NumberFormatException ex) {
				System.out.println("Enter an integer buddy");
				System.exit(1);
			}
		}
	}
}