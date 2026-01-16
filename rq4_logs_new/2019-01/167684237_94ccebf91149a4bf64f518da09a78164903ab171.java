
public class Main {

	public static void main(String[] args) {
		System.out.println(getLongestRepeatChar("aaabbbcddddd"));

	}
	
	
	public static String getLongestRepeatChar(String input) {
		
		
		int count = 1;
		String tempMax = String.valueOf(input.charAt(0));
		String maxString = tempMax;
		
		for (int i = 1; i < input.length(); i++) {
			
			if (input.charAt(i-1)==input.charAt(i)) {
				count++;
				tempMax = tempMax + String.valueOf(input.charAt(i));
				}			
			else {
				tempMax = String.valueOf(input.charAt(i));
				count = 1;
			}
			if (count>maxString.length()) {
				maxString = tempMax;
			}
		}
		
		return maxString;
	}
	
}