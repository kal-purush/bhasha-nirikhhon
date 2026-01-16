import java.util.Scanner;

public class Main {
	public static void main(String args[]) {
		Scanner sc = new Scanner(System.in);
		int n = sc.nextInt();
		int count = 0;
		int oneCount = 0;
		int zeroCount = 0;
		
		for (int i = 0; i < n; i++) {
			String c = sc.next();
			if (c.equals("0")) {
				zeroCount++;
			} else {
				oneCount++;
			}
			if (zeroCount > oneCount) {
				count += zeroCount;
				zeroCount = oneCount = 0;
			}
		}
		count += oneCount;
		System.out.println(count);
	}
}