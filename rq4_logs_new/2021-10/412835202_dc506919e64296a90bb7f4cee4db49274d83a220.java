
public class ASCII_difference {
	
	private static String checkDifference(String name) {
		// TODO Auto-generated method stub
		String st = "";
		for(int i=1 ; i<name.length() ; i++) {
			st += name.charAt(i-1);
			char cur = name.charAt(i);
			char prev = name.charAt(i-1);
			int diff = cur - prev;
			st += diff;
		}
		st += name.charAt(name.length()-1);
		return st;
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String name = "abdgca";
		System.out.println(checkDifference(name));
	}

}