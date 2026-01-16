import java.util.Arrays;

/* In England the currency is made up of pound, �, and pence, p, and
 * there are eight coins in general circulation:
 * 
 * 1p, 2p, 5p, 10p, 20p, 50p, �1 (100p) and �2 (200p).
 * 
 * It is possible to make �2 in the following way:
 * 
 * 1ף1 + 1�50p + 2�20p + 1�5p + 1�2p + 3�1p
 * 
 * How many different ways can �2 be made using any number of coins? */

public class N031CoinSums2 {
	
	public static int[] coinValues = {1, 2, 5, 10, 20, 50, 100, 200};
	
	public static int amount = 200;
	
	public static int[][] memo = new int[amount + 1][coinValues.length];
	
	public static int countWays(int sum, int coinIndex) {		
		if (sum == 0 | coinIndex == 0) {
			return 1;
		}
		
		if (memo[sum][coinIndex] != 0) {
			return memo[sum][coinIndex];
		}
		
		int ways = 0;
		int div = sum / coinValues[coinIndex];
		for (int i = 0; i <= div; ++i) {
			ways += countWays(sum - (i * coinValues[coinIndex]), coinIndex - 1);
		}
		
		memo[sum][coinIndex] = ways;
		return memo[sum][coinIndex];
	}
	
	
	public static void main(String[] args) {
		double start = System.currentTimeMillis();
		
		System.out.println(countWays(amount, coinValues.length - 1));
		
		double end = System.currentTimeMillis();
		double time = end - start;
		System.out.println("program runtime was " + time + " ms.");
	}
}